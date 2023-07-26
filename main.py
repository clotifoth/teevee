
from io import BytesIO
import json
import os
import subprocess
from fastapi.responses import HTMLResponse, StreamingResponse
import time, requests
from fastapi import HTTPException
from fastapi import FastAPI
import logging
import pandas as pd
import random
import xspf_lib
from ffprobe import FFProbe

# @TODO: Fulsome logging policy implemented via logging module
logger = logging.getLogger(__name__)
app = FastAPI()

commercial_library = dict()
bump_library = dict()
show_library = None

schedule_library = dict()
xspf_library = dict()

def preload_bump_info(bump_library_path_list):
    for bump_library_path in bump_library_path_list:
        if(bump_library_path in bump_library):
            continue
        bumps = []  
        count = 0
        for bump in os.listdir(bump_library_path):
            if bump.endswith(".mp4") or bump.endswith(".mkv") or bump.endswith(".mov") or bump.endswith(".avi") or bump.endswith(".webm"):
                full_path = os.path.join(bump_library_path, bump)
                duration = get_length(full_path)
                bumps.append(
                {
                    "type": "bump",
                    "name": str(bump),
                    "path": full_path,
                    "duration": duration
                } )
                count = count + 1
                if(count % 100 == 0):
                    print(count)
        bump_library[bump_library_path] = bumps

def preload_commercial_info(commercial_library_path_list):
    for commercial_library_path in commercial_library_path_list:
        if(commercial_library_path in commercial_library):
            continue
        commercials = []
        count = 0
        for commercial in os.listdir(commercial_library_path):
            if commercial.endswith(".mp4") or commercial.endswith(".mkv") or commercial.endswith(".mov") or commercial.endswith(".avi") or commercial.endswith(".webm"):
                full_path = os.path.join(commercial_library_path, commercial)
                duration = get_length(full_path)
                commercials.append(
                {
                    "type": "commercial",
                    "name": str(commercial),
                    "path": full_path,
                    "duration": duration
                } )
                count = count + 1
                if(count % 100 == 0):
                    print(count)
        commercial_library[commercial_library_path] = commercials

def preload_show_info(show_library_path):
    show_library = dict()
    count = 0
    for show_season_folder in os.listdir(show_library_path):
        show_library[show_season_folder] = []
        for show in os.listdir(os.path.join(show_library_path, show_season_folder)):
            if show.endswith(".mp4") or show.endswith(".mkv") or show.endswith(".mov") or show.endswith(".avi") or show.endswith(".webm"):
                full_path = os.path.join(show_library_path, show_season_folder, show)
                duration = get_length(full_path)
                show_library[show_season_folder].append(
                {
                    "type": "episode",
                    "show": show_season_folder,
                    "name": show,
                    "path": os.path.join(show_library_path, show_season_folder, show),
                    "duration": duration
                })
                count = count + 1
                if(count % 100 == 0):
                    print(count)
    return show_library



def select_movie_file(path):
    movie = random.choice(path)
    return movie

def generate_schedule(channel_name, repeat: int = 1):
    schedule = []
    if channel_name not in channels_library:
        return None
    channel = channels_library[channel_name]
    commercials_paths = channel["commercial_library_paths"]
    bumps_paths = channel["bump_library_paths"]
    for i in range(0, repeat):
        for time_slot in channel["block_ordering"]:
            commercials = []
            if time_slot == "*":
                commercial_length = 150
            else:
                commercial_length = channel["segment_types"][time_slot]["commercials_length"]
            while commercial_length > 0:
                commercial = None
                while(commercial is None or commercial == commercials[:1]):
                    commercials_path = random.choice(commercials_paths)
                    commercial = select_movie_file(commercial_library[commercials_path])
                commercials.append(commercial)
                commercial_length = commercial_length - commercial["duration"]
            if time_slot == "*":
                possible_show_selections = list(show_library)
            else:
                possible_show_selections = []
                for show in channel["segment_types"][time_slot]["show_map"].keys():
                    show_config = channel["segment_types"][time_slot]["show_map"][show]
                    if("shows" in show_config):
                        for show_title in show_config["shows"]: possible_show_selections.append(show_title) 
                    else:
                        possible_show_selections.append(show)   
            show = []
            while len(show) == 0:
                show = random.choice(possible_show_selections)
                
            bumps_path = random.choice(bumps_paths)
            intro_bump = select_movie_file(bump_library[bumps_path])
            outro_bump = select_movie_file(bump_library[bumps_path])

            episode = select_movie_file(show_library[show])
            # Assemble show block
            for commercial in commercials:
                schedule.append(commercial)
            schedule.append(intro_bump)
            schedule.append(episode)
            schedule.append(outro_bump)
    return schedule

def get_length(input_video):
    result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_video], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        result = float(result.stdout.decode().split("\r\n")[0])
        return result
    except Exception as e:
        try:
            if(result == ''):
                return 0
            result = float(result.stdout.decode().split("\r\n")[1])
            return result
        except Exception as e:
            print(e)
            return 0

def render_schedule(schedule):
    tracks = []
    for track in schedule:
        path = track["path"]
        if track["duration"] is None:
            duration = 0
        else:
            duration = track["duration"]*1000
        track = xspf_lib.Track(location=track["path"],
                                title=".",
                                creator="[schmedult schmim]",
                                album=track["show"] if "show" in track else track["type"],
                                duration=duration,
                                annotation=track["name"],
                                info="",
                                image="")
        tracks.append(track)
    playlist = xspf_lib.Playlist(title="Schedule",
                             creator="[schmedult schmim]",
                             annotation="",
                             trackList=tracks)
    return playlist

@app.get("/schedule", response_class=HTMLResponse)
async def respond_with_schedule(channel_name: str, schedule_id: int = None):
    if(schedule_id is not None and schedule_id in schedule_library):
        schedule = schedule_library[schedule_id]
        rendered_schedule = xspf_library[schedule_id]
    else:
        while schedule_id is None or schedule_id in schedule_library:
            schedule_id = random.randint(100000000,199999999)
        schedule = generate_schedule(channel_name)
        schedule_library[schedule_id] = schedule
        rendered_schedule = render_schedule(schedule)
        xspf_library[schedule_id] = rendered_schedule

    schedule_visualization_df = pd.DataFrame(schedule, columns=["type","show","name","path","duration"])
    schedule_visualization_df['Elapsed Time'] = schedule_visualization_df['duration'].cumsum()

    schedule_visualization_df['Elapsed Time'] = pd.to_datetime(schedule_visualization_df['duration'].cumsum().sub(schedule_visualization_df.duration), unit='s').dt.strftime("Day %d %H:%M:%S")
    schedule_visualization_df.drop(schedule_visualization_df[schedule_visualization_df['type'] == "commercial"].index, inplace = True)
    highlighted_rows = schedule_visualization_df['type'].isin(['commercial','bump']).map({
        True: 'font-size: 8px; font-style: italic;',
        False: 'border: 2px solid lightgrey; border-collapse: collapse;'
    })
    
    # Apply calculated styles to each column:
    styler = schedule_visualization_df.style.apply(lambda _: highlighted_rows)
    
    return f"""
        <html>
            <head>
            <style>
            </style>
            </head>
            <body>
                <h2>schedule {schedule_id} - <a href='./download/{schedule_id}.xspf'>(.XSPF)</a> - <a href='./download/{schedule_id}.json'>(.JSON) - <a href=./schedule?channel_name={channel_name}&schedule_id={schedule_id}>permalink</a></h2>
                {styler.to_html(formatters={'name': lambda x: '<b>' + x + '</b>'})}
            </body>
        </html>
    """


@app.get("/download/{schedule_id}.{format}", response_class=StreamingResponse)
async def download_schedule(schedule_id: int, format: str):
    filtered_image = BytesIO()
    if format == "xspf":
        rendered_schedule = xspf_library[schedule_id]
        rendered_schedule.write(filtered_image)
    elif format == "json":
        rendered_schedule = {"schedule": schedule_library[schedule_id]}
        filtered_image.write(str(rendered_schedule).encode('utf-8'))
    filtered_image.seek(0)
    if rendered_schedule:
        return StreamingResponse(filtered_image, media_type="file/xml", headers={'Content-Disposition': f'attachment; filename="schedule.{format}"'})
    else:
        raise HTTPException(status_code=404, detail=f"404")

# @app.get("/play", response_class=StreamingResponse)
# async def play(length: int=16):
#     schedule = generate_schedule(length=length)
#     rendered_schedule = render_schedule(schedule)
#     filtered_image = BytesIO()
#     rendered_schedule.write(filtered_image)
#     filtered_image.seek(0)
#     if rendered_schedule:
#         return StreamingResponse(filtered_image, media_type="file/xml", headers={'Content-Disposition': 'attachment; filename="schedule.xspf"'})
#     else:
#         raise HTTPException(status_code=404, detail=f"404")
    
def load_channels(channels_path: str):
    channels_library = dict()
    for channel_config in os.listdir(channels_path):
        channel_name = channel_config.split(".")[0]
        if channel_config.endswith(".json"):
            full_path = os.path.join(channels_path, channel_config)
            with open(full_path) as file:
                channel = json.load(file)
            channels_library[channel_name] = channel
            preload_commercial_info(channel["commercial_library_paths"])
            preload_bump_info(channel["bump_library_paths"])
    return channels_library


show_library_path = "E:\\legbreak-content\\shows"

cached_library_path = "./cache/"
bump_cache = os.path.join(cached_library_path, "bump_library.json")
try:
    with(open(bump_cache) as fp):
        bump_library = json.load(fp)
        for bump_path in bump_library:
            bump_folder = bump_library[bump_path]
            for bump in bump_folder:
                if(not os.path.isfile(bump["path"])):
                    logger.info(f"Cache invalided because {bump['path']} not found - redoing bumps cache")
                    bump_library = dict()
                    break
except FileNotFoundError as e:
    pass

commercials_cache = os.path.join(cached_library_path, "commercials_library.json")
try:
    with(open(commercials_cache) as fp):
        commercial_library = json.load(fp)
        for commercial_path in commercial_library:
            commercial_folder = commercial_library[commercial_path]
            for commercial in commercial_folder:
                if(not os.path.isfile(commercial["path"])):
                    logger.info(f"Cache invalided because {commercial['path']} not found - redoing bumps cache")
                    commercial_library = dict()
                    break
except FileNotFoundError as e:
    pass

shows_cache = os.path.join(cached_library_path, "shows_library.json")
try:
    if(os.path.exists(shows_cache)):
        with(open(shows_cache) as fp):
            show_library = json.load(fp)
        for show_season_folder in show_library:
            print(show_season_folder)
            show_season = show_library[show_season_folder]
            for show in show_season:
                #show = show_season[file]
                if(not os.path.isfile(show["path"])):
                    logger.info(f"Cache invalided because {show['path']} not found - redoing bumps cache")
                    show_library = None
                    break
except FileNotFoundError as e:
    pass

channels_path = "./channels/"
channels_library = load_channels(channels_path)

with open(bump_cache, mode="w+") as bump_file:
    json.dump(bump_library, bump_file)
with open(commercials_cache, mode="w+") as commercial_file:
    json.dump(commercial_library, commercial_file)

if show_library is None:
    show_library = preload_show_info(show_library_path)
with open(shows_cache, mode="w+") as show_file:
    json.dump(show_library, show_file)

