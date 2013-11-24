#  This file is part of Headphones.
#
#  Headphones is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Headphones is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Headphones.  If not, see <http://www.gnu.org/licenses/>.

import os
import glob
import datetime
import time

from lib.beets.mediafile import MediaFile
import lib.beets.library as beets

import headphones
from headphones import db, logger, helpers, importer

# You can scan a single directory and append it to the current library by specifying append=True, ArtistID & ArtistName
def libraryScan(dir=None, append=False, ArtistID=None, ArtistName=None, cron=False):

    if cron and not headphones.LIBRARYSCAN:
        return
        
    if not dir:
        if not headphones.MUSIC_DIR:
            return
        else:
            dir = headphones.MUSIC_DIR
    
    # If we're appending a dir, it's coming from the post processor which is already bytestring
    if not append:
        dir = dir.encode(headphones.SYS_ENCODING)

    unicode_dir = dir.decode(headphones.SYS_ENCODING, 'replace')

    if not os.path.isdir(dir):
        logger.warn('Cannot find directory: %s. Not scanning' % unicode_dir)
        return

    myDB = db.DBConnection()
    
    if not append:
        logger.info("checking existing tracks")

        # Clean up bad filepaths
        tracks = myDB.select('SELECT Location, TrackID from tracks WHERE Location IS NOT NULL')
    
        for track in tracks:
            if not os.path.isfile(track['Location'].encode(headphones.SYS_ENCODING)):
                myDB.action('UPDATE tracks SET Location=?, BitRate=?, Format=? WHERE TrackID=?', [None, None, None, track['TrackID']])

        myDB.action('DELETE from have')

    using_beets_db = headphones.USE_BEETS_DB and not append
    if using_beets_db:
        song_list, bitrates = scanBeetsDB(dir)
    else:
        song_list, bitrates = scanDirectory(dir)

    # Now we start track matching
    total_number_of_songs = len(song_list)
    logger.info("Found %d tracks; matching tracks to the appropriate releases..." % total_number_of_songs)
    
    # Sort the song_list by most vague (e.g. no trackid or releaseid) to most specific (both trackid & releaseid)
    # When we insert into the database, the tracks with the most specific information will overwrite the more general matches
    song_list = helpers.multikeysort(song_list, ['ReleaseID', 'TrackID'])

    new_artists = []
    
    # We'll use this to give a % completion, just because the track matching might take a while
    song_count = 0
    time_last = time.time()
    start_time = time_last

    for song in song_list:
        song_count, time_last = log_track_matching_progress(song_count, time_last, start_time, total_number_of_songs)

        newSongDict = {  'Location' : song['Location'],
                         'BitRate'  : song['BitRate'],
                         'Format'   : song['Format'] }

        cleanName = helpers.cleanName(song['ArtistName'], song['AlbumTitle'], song['TrackTitle'])

        all_mb_info_present = song['TrackID'] and song['ReleaseID']

        if matchByIDs(myDB, song, newSongDict):
            continue

        #The rest of the matching checks can be avoided if we're using the beets DB and all info is present;
        #it's almost certain that this is a new artist and the rest of the checks won't find anything.
        #In other words, when using Beets, trust the tags more.
        can_skip_extra_checks = using_beets_db and all_mb_info_present

        if not can_skip_extra_checks:
            if   matchByReleaseIdAndTitle(    myDB, song, newSongDict) or \
                 matchByTrackIDAndAlbumTitle( myDB, song, newSongDict) or \
                 matchByTitles(               myDB, song, newSongDict) or \
                 matchByCleanName(            myDB, song, newSongDict, cleanName) or \
                 matchByTrackID(              myDB, song, newSongDict):
               continue
        
        # if we can't find a match in the database on a track level, it might be a new artist or it might be on a non-mb release
        if song['ArtistName']:
            new_artists.append(song['ArtistName'])
        
        have_important_data = song['ArtistName'] and song['AlbumTitle'] and song['TrackTitle']
        if not have_important_data:
            continue
        
        # The have table will become the new database for unmatched tracks (i.e. tracks with no associated links in the database)
        myDB.insert('have', {
            'ArtistName':   song['ArtistName'],
            'AlbumTitle':   song['AlbumTitle'],
            'TrackNumber':  song['TrackNumber'],
            'TrackTitle':   song['TrackTitle'],
            'TrackLength':  song['TrackLength'],
            'BitRate':      song['BitRate'],
            'Genre':        song['Genre'],
            'Date':         song['Date'],
            'TrackID':      song['TrackID'],
            'Location':     song['Location'],
            'CleanName':    cleanName,
            'Format':       song['Format']
        })

    logger.info('Completed matching tracks from directory: %s' % unicode_dir)
    
    if not append:
        # Clean up the new artist list
        unique_artists = {}.fromkeys(new_artists).keys()
        current_artists = myDB.select('SELECT ArtistName, ArtistID from artists')
        
        artist_list = [f for f in unique_artists if f.lower() not in [x[0].lower() for x in current_artists]]
        
        # Update track counts
        logger.info('Updating current artist track counts')
    
        for artist in current_artists:
            havetracks = importer.countHaveTracks(artist['ArtistID'], artist['ArtistName'])
            myDB.action('UPDATE artists SET HaveTracks=? WHERE ArtistID=?', [havetracks, artist['ArtistID']])
            
        logger.info('Found %i new artists' % len(artist_list))
    
        if len(artist_list):
            if headphones.ADD_ARTISTS:
                logger.info('Importing %i new artists' % len(artist_list))
                importer.artistlist_to_mbids(artist_list)
            else:
                logger.info('To add these artists, go to Manage->Manage New Artists')
                myDB.action('DELETE from newartists')
                for artist in artist_list:
                    myDB.action('INSERT into newartists VALUES (?)', [artist])
        
        if headphones.DETECT_BITRATE:
            headphones.PREFERRED_BITRATE = sum(bitrates)/len(bitrates)/1000
            
    else:
        # If we're appending a new album to the database, update the artists total track counts
        logger.info('Updating artist track counts')
        
        havetracks = importer.countHaveTracks(ArtistID, ArtistName)
        myDB.action('UPDATE artists SET HaveTracks=? WHERE ArtistID=?', [havetracks, ArtistID])





def count_have_tracks(myDB, artistId, artistName):
    # Have tracks are selected from tracks table and not from alltracks because of duplicates
    # We update the track count upon an album switch to compliment this
    return myDB.select('SELECT count(1) from tracks WHERE ArtistID=? AND Location IS NOT NULL', [artistID]).fetchone()[0] + \
           myDB.select('SELECT count(1) from have WHERE ArtistName like ?', [artistName]).fetchone()[0]

#idiomatization for a bunch of queries that happened a lot of times
def select_from_tracks_or_all_tracks(myDB, select, where, args):
    return myDB.action(select+' from alltracks '+where, args).fetchone() or \
           myDB.action(select+' from tracks    '+where, args).fetchone()


def log_track_matching_progress(song_count, time_last, start_time, total_number_of_songs):
    if song_count > 0:
        now = time.time()
        last = (now - time_last) * 1000
        avg = ((now - start_time) * 1000) / song_count
        remaining = (total_number_of_songs - song_count) * ((now - start_time) / song_count) / 3600.0
        #logger.debug("last track: %dms - avg: %dms - remaining: %0.2fhs" % (last, avg, remaining))
        time_last = now

    song_count += 1

    if song_count % 100 == 0:
        completion_percentage = float(song_count)/total_number_of_songs * 100
        logger.info("Track matching: %d of %d - %.2f%% - average per track: %dms - remaining: %0.2fhs" % \
            (song_count, total_number_of_songs, completion_percentage, avg, remaining))

    return song_count, time_last



def scanDirectory(dir):
    logger.info('Scanning music directory: %s' % dir)

    bitrates = []
    song_list = []

    for r,d,f in os.walk(dir):
        #need to abuse slicing to get a copy of the list, doing it directly will skip the element after a deleted one
        #using a list comprehension will not work correctly for nested subdirectories (os.walk keeps its original list)
        for directory in d[:]:
            if directory.startswith("."):
                d.remove(directory)
        for file_path in f:
            #skip files with formats we don't care about
            if not any(file_path.endswith('.' + x.lower()) for x in headphones.MEDIA_FORMATS):
                continue

            # We need the unicode path to use for logging, inserting into database
            abs_path = os.path.join(r, file_path)
            unicode_path = abs_path.decode(headphones.SYS_ENCODING, 'replace')

            # Try to read the metadata
            try:
                f = MediaFile(unicode_path)
            except:
                logger.error('Cannot read file: ' + unicode_path)
                continue

            # Grab the bitrates for the auto detect bit rate option
            if headphones.DETECT_BITRATE and f.bitrate:
                bitrates.append(f.bitrate)

            # Add the song to our song list -
            # TODO: skip adding songs without the minimum requisite information (just a matter of putting together the right if statements)

            song_dict = { 'TrackID'   :  f.mb_trackid,
                          'ReleaseID' :  f.mb_albumid,
                          # Use the album artist over the artist if available
                          'ArtistName' : f.albumartist or f.artist,
                          'AlbumTitle' : f.album,
                          'TrackNumber': f.track,
                          'TrackLength': f.length,
                          'Genre'      : f.genre,
                          'Date'       : f.date,
                          'TrackTitle' : f.title,
                          'BitRate'    : f.bitrate,
                          'Format'     : f.format,
                          'Location'   : unicode_path }

            song_list.append(song_dict)

    return song_list, bitrates

def scanBeetsDB(dir):
    bitrates = []
    song_list = []

    db_path = headphones.BEETS_DB_PATH
    if not os.path.isfile(db_path):
        logger.error("could not find beets library in %s; aborting" % db_path)
        return song_list, bitrates

    logger.info('Reading beets database %s to get music from %s' % (db_path, dir))
    beets_db = beets.Library(path=db_path)

    for f in beets_db.items(beets.PathQuery(dir)):
        #skip files with formats we don't care about
        if not any(f.path.lower().endswith('.' + x.lower()) for x in headphones.MEDIA_FORMATS):
            continue

        # We need the unicode path to use for logging, inserting into database
        unicode_path = f.path.decode(headphones.SYS_ENCODING, 'replace')

        # Grab the bitrates for the auto detect bit rate option
        if headphones.DETECT_BITRATE and f.bitrate:
            bitrates.append(f.bitrate)

        try:
            f_date = datetime.date(
                max(f.year,  datetime.MINYEAR),
                max(f.month, 1),
                max(f.day, 1)
            )
        except ValueError:  # Out of range values.
            f_date = datetime.date.min

        # Add the song to our song list -
        # TODO: skip adding songs without the minimum requisite information (just a matter of putting together the right if statements)

        song_dict = { 'TrackID'    : f.mb_trackid,
                      'ReleaseID'  : f.mb_albumid,
                      # Use the album artist over the artist if available
                      'ArtistName' : f.albumartist or f.artist,
                      'AlbumTitle' : f.album,
                      'TrackNumber': f.track,
                      'TrackLength': f.length,
                      'Genre'      : f.genre,
                      'Date'       : f_date,
                      'TrackTitle' : f.title,
                      'BitRate'    : f.bitrate,
                      'Format'     : f.format,
                      'Location'   : unicode_path }

        song_list.append(song_dict)

    return song_list, bitrates







#----- track matching methods:

def matchByIDs(myDB, song, newSongDict):
    # If the track has a trackid & releaseid (beets: albumid), that's the most surefire way
    # of identifying a track to a specific release so we'll use that first
    if song['TrackID'] and song['ReleaseID']:

        # Check both the tracks table & alltracks table in case they haven't populated the alltracks table yet
        # It might be the case that the alltracks table isn't populated yet, so maybe we can only find a match in the tracks table
        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT TrackID, ReleaseID, AlbumID',
            'WHERE TrackID=? AND ReleaseID=?',
            [song['TrackID'], song['ReleaseID']])

        if track:
            # Use TrackID & ReleaseID here since there can only be one possible match with a TrackID & ReleaseID query combo
            controlValueDict = { 'TrackID'   : track['TrackID'],
                                 'ReleaseID' : track['ReleaseID'] }

            # Insert it into the Headphones hybrid release (ReleaseID == AlbumID)
            hybridControlValueDict = { 'TrackID'   : track['TrackID'],
                                       'ReleaseID' : track['AlbumID'] }

            # Update both the tracks table and the alltracks table using the controlValueDict and hybridControlValueDict
            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            myDB.upsert("alltracks", newSongDict, hybridControlValueDict)
            myDB.upsert("tracks", newSongDict, hybridControlValueDict)

            return True

def matchByReleaseIdAndTitle(myDB, song, newSongDict):
    # If we can't find it with TrackID & ReleaseID, next most specific will be
    # releaseid + tracktitle, although perhaps less reliable due to a higher
    # likelihood of variations in the song title (e.g. feat. artists)
    if song['ReleaseID'] and song['TrackTitle']:

        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT TrackID, ReleaseID, AlbumID',
            'WHERE ReleaseID=? AND TrackTitle=?',
            [song['ReleaseID'], song['TrackTitle']])

        if track:
            # There can also only be one match for this query as well (although it might be on both the tracks and alltracks table)
            # So use both TrackID & ReleaseID as the control values
            controlValueDict = { 'TrackID'   : track['TrackID'],
                                 'ReleaseID' : track['ReleaseID'] }

            hybridControlValueDict = { 'TrackID'   : track['TrackID'],
                                       'ReleaseID' : track['AlbumID'] }

            # Update both tables here as well
            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            myDB.upsert("alltracks", newSongDict, hybridControlValueDict)
            myDB.upsert("tracks", newSongDict, hybridControlValueDict)

            return True

def matchByTrackIDAndAlbumTitle(myDB, song, newSongDict):
    # Next most specific will be the opposite: a TrackID and an AlbumTitle
    # TrackIDs span multiple releases so if something is on an official album
    # and a compilation, for example, this will match it to the right one
    # However - there may be multiple matches here
    if song['TrackID'] and song['AlbumTitle']:

        # Even though there might be multiple matches, we just need to grab one to confirm a match
        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT TrackID, AlbumTitle',
            'WHERE TrackID=? AND AlbumTitle LIKE ?',
            [song['TrackID'], song['AlbumTitle']])

        if track:
            # Don't need the hybridControlValueDict here since ReleaseID is not unique
            controlValueDict = { 'TrackID'   : track['TrackID'],
                                 'AlbumTitle' : track['AlbumTitle'] }

            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            return True

def matchByTitles(myDB, song, newSongDict):
    # Next most specific is the ArtistName + AlbumTitle + TrackTitle combo (but probably
    # even more unreliable than the previous queries, and might span multiple releases)
    if song['ArtistName'] and song['AlbumTitle'] and song['TrackTitle']:

        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT ArtistName, AlbumTitle, TrackTitle',
            'WHERE ArtistName LIKE ? AND AlbumTitle LIKE ? AND TrackTitle LIKE ?',
            [song['ArtistName'], song['AlbumTitle'], song['TrackTitle']])

        if track:
            controlValueDict = { 'ArtistName' : track['ArtistName'],
                                 'AlbumTitle' : track['AlbumTitle'],
                                 'TrackTitle' : track['TrackTitle'] }

            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            return True

def matchByCleanName(myDB, song, newSongDict, cleanName):
    # Use the "CleanName" (ArtistName + AlbumTitle + TrackTitle stripped of punctuation, capitalization, etc)
    # This is more reliable than the former but requires some string manipulation so we'll do it only
    # if we can't find a match with the original data
    if song['ArtistName'] and song['AlbumTitle'] and song['TrackTitle']:

        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT CleanName',
            'WHERE CleanName LIKE ?',
            [cleanName])

        if track:
            controlValueDict = { 'CleanName' : track['CleanName'] }

            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            return True

def matchByTrackID(myDB, song, newSongDict):
    # Match on TrackID alone if we can't find it using any of the above methods. This method is reliable
    # but spans multiple releases - but that's why we're putting at the beginning as a last resort. If a track
    # with more specific information exists in the library, it'll overwrite these values
    if song['TrackID']:

        track = select_from_tracks_or_all_tracks(myDB,
            'SELECT TrackID',
            'WHERE TrackID=?',
            [song['TrackID']])

        if track:
            controlValueDict = { 'TrackID' : track['TrackID'] }

            myDB.upsert("alltracks", newSongDict, controlValueDict)
            myDB.upsert("tracks", newSongDict, controlValueDict)

            return True

