import os
import pandas as pd
from fastparquet import write
import re
import datetime as dt
from pathlib import Path
import socket

sock = socket.socket()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

API_ENDPOINT = str("https://id.twitch.tv/oauth2/token")
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
token = os.getenv("OAUTH_SECRET")
nickname = os.getenv("TWITCH_USERNAME")
filename = input("Enter a twitch channel ( i.e xqc ) : ")
channel = "#" + filename
server = 'irc.chat.twitch.tv'
port = 6667

directory = Path(__file__).resolve().parent
state = True


while state:
    sock.connect((server, port))
    sock.send(f"PASS {token}\n".encode('utf-8'))
    sock.send(f"NICK {nickname}\n".encode('utf-8'))
    sock.send(f"JOIN {channel}\n".encode('utf-8'))
    try:
        try:
            try:
                while True:
                    resp = sock.recv(2048).decode(encoding="utf-8", errors="ignore")
                    # print(str(count)+"\n")
                    if resp.startswith("PONG"):
                        sock.send("PONG\n".encode('utf-8'))
                    elif len(resp) > 0:
                        test = re.split(r"\r\n:|\r\n", resp)
                        # print(len(test))
                        # print(test)
                        for i in test:
                            if len(i) > 0:
                                user = re.split(rf"PRIVMSG|#{filename}|!", i)
                                # print(user)
                                if len(user) == 4:
                                    try:
                                        # print(user)
                                        now = dt.datetime.now()
                                        timestamp = dt.datetime.timestamp(now)
                                        if user[0].__contains__(":"):
                                            username_list = user[0].split(":")
                                            username = username_list[1]
                                        else:
                                            username = user[0]

                                        chat_list = user[3].split(":")
                                        chat = chat_list[1]

                                        if username.__contains__(nickname):
                                            print(bcolors.OKBLUE + username + " : " + chat + bcolors.ENDC)
                                        else:
                                            print(username + " : " + chat)

                                        data = {'Username': [username], 'Chat': [chat], "Timestamp": [timestamp]}
                                        df = pd.DataFrame(data)
                                        if not os.path.exists(f'{directory}/parq_file/{filename}.parq'):
                                            write(f'{directory}/parq_file/{filename}.parq', df)
                                        else:
                                            write(f'{directory}/parq_file/{filename}.parq', df, append=True)
                                    except IndexError:
                                        pass
            except ConnectionResetError:
                print("Reconnect")
        except UnicodeDecodeError:
            print("Unicode Decode Error Reconnect")

    except KeyboardInterrupt:
        state = False
