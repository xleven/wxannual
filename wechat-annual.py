import argparse
import hashlib
import http.server
import json
import logging
import os
import platform
import re
import socket
import sqlite3
import sys
import webbrowser
from datetime import datetime
from functools import partial
from itertools import product
from pathlib import Path

import pandas as pd
import segno
from jinja2 import Environment, FileSystemLoader
from lxml import etree

    
def username_to_md5(username: str) -> str:
    return hashlib.md5(username.encode('utf-8')).hexdigest()

def parse_blob(blob, regex):
    matches = re.findall(regex, blob)
    if len(matches) > 0:
        result = matches[0].decode()
    else:
        result = None
    return result

def parse_xml_msg(msg, path='appmsg/type', attr=None):
    try:
        # 指定parser，且尝试解析有误的XML
        xml = etree.XML(msg, parser=etree.XMLParser(remove_blank_text=True, recover=True))
        ele = xml.xpath(path)[0]
        t = ele.get(attr) if attr else ele.text
        if t == '':
            t = None
    except:
        t = None
    return t

def pause_for_exit():
    input("Press <ENTER> to exit")
    sys.exit()

def get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("114.114.114.114", 80))
    lanip = s.getsockname()[0]
    s.close()
    return lanip


class WeChat:
    def __init__(self, backup_dir: Path = None) -> None:
        if backup_dir:
            backdir = backup_dir
        elif platform.system() == "Darwin":
            backdir = Path.home() / Path("Library/Application Support/MobileSync/Backup")
        elif platform.system() == "Windows":
            dir_app = [Path(os.getenv("APPDATA")), Path(os.getenv("USERPROFILE"))]
            dir_bak = ["Apple/MobileSync/Backup", "Apple Computer/MobileSync/Backup"]
            for app, bak in product(dir_app, dir_bak):
                backdir = app / bak
                if backdir.is_dir():
                    break
        else:
            logger.error("OS not supported!")
            pause_for_exit()
        logger.debug(f"Backup directory: {backdir}")
        try:
            self.backup = sorted(backdir.glob("[!.]*"), key=os.path.getatime)[-1]
        except IndexError:
            logger.error("iTunes backup not found!")
            pause_for_exit()

        now = datetime.now()  # datetime() with tzinfo included
        self.year = now.year - 1 if now.month < 3 else now.year
        self.st = datetime(self.year,1,1).timestamp()
        self.et = datetime(self.year+1,1,1).timestamp()

    def prepare_wxfile(self):
        logger.debug("Searching manifest for WeChat files")
        try:
            with sqlite3.connect(self.backup / "Manifest.db") as conn:
                wxfile = pd.read_sql("""
                    SELECT relativePath, fileID
                    FROM Files
                    WHERE domain='AppDomain-com.tencent.xin' AND flags=1
                """, conn)
        except Exception as err:
            logger.error("WeChat files not found!")
            pause_for_exit()
        else:
            self.wxfile = wxfile

    def get_file_by_id(self, file_id: str) -> Path:
        file_path = self.backup / file_id[:2] / file_id
        return file_path

    def get_file_by_path(self, path: str) -> Path:
        file_id = self.wxfile[self.wxfile["relativePath"]==path]["fileID"].iat[0]
        file_path = self.get_file_by_id(file_id)
        return file_path
        
    def prepare_myself_info(self):
        logger.debug("Parsing self account info")
        mmptn = "^Documents/MMappedKV/mmsetting.archive.[0-9A-Za-z_\-]{6,20}$"
        wxmm = self.wxfile[self.wxfile["relativePath"].str.match(mmptn)]["relativePath"]
        mymmp = wxmm.iat[0]
        self.myid = mymmp.split(".")[-1]
        self.mymd5 = username_to_md5(self.myid)
        try:
            mymmb = self.get_file_by_path(mymmp).read_bytes()
            myname = re.findall(b"88[\x00-\x2f]{2}(.*?)\x01", mymmb)[0].decode()
            myheadimg = re.findall(b"headimgurl.*(https?://.*?/.*?/(?:.*?/)?.*?/\d+)", mymmb)[0].decode()
        except Exception as err:
            self.myname, self.myheadimg = "微信用户", ""
            logger.warning(f"Failed due to {err}. Fallback to default")
        else:
            self.myname, self.myheadimg = myname, myheadimg

    def prepare_contact(self):
        logger.debug("Parsing WeChat contacts")
        contact_db = self.get_file_by_path("Documents/{}/DB/WCDB_Contact.sqlite".format(self.mymd5))

        with sqlite3.connect(contact_db) as conn:
            friends = pd.read_sql("""
                SELECT userName, type, dbContactRemark, dbContactHeadImage, dbContactProfile
                FROM Friend
                WHERE type % 2 = 1 AND dbContactEncryptSecret NOT NULL
            """, conn, index_col="userName")
            groups = pd.read_sql("""
                SELECT userName, type, dbContactRemark, dbContactChatRoom
                FROM Friend
                WHERE userName LIKE "%@chatroom"
            """, conn, index_col="userName")

        remark_fields = {
            10: "nickname",
            18: "id_new",
            26: "alias",
            34: "alias_pinyin",
            42: "alias_PY",
            50: "nickname_pinyin",
            58: "description",
            66: "tag",
        }

        def parse_remark(blob):
            csor, data = 0, {}
            while csor < len(blob):
                dtype = blob[csor]
                csor += 1
                step = blob[csor]
                csor += 1
                data[dtype] = blob[csor:csor+step].decode()
                csor += step
            return pd.Series(data).rename(remark_fields)

        parse_headimg = partial(parse_blob, regex=b"https?://.*?/.*?/(?:.*?/)?.*?/\d+")
        parse_founder = partial(parse_blob, regex=b"\x12.([0-9A-Za-z_\-]{6,20})")
        parse_chatroom = partial(parse_blob, regex=b"<RoomData>.*</RoomData>")
        parse_profile = partial(parse_blob, regex=b"\x08([\x00-\x02])")

        try:
            logger.debug("Parsing friends out of contacts")
            friends = friends.join(friends["dbContactRemark"].apply(parse_remark))
            friends["headimg"] = friends["dbContactHeadImage"].map(parse_headimg)
            friends["gender"] = friends["dbContactProfile"].map(parse_profile)
            friends["table"] = "Chat_" + friends.index.map(username_to_md5)
        except Exception as err:
            logger.warning(f"Failed due to {err}")
        else:
            self.friends = friends.drop(columns=friends.filter(like="dbContact").columns)

        try:
            logger.debug("Parsing groups out of contacts")
            groups = groups.join(groups["dbContactRemark"].apply(parse_remark))
            groups["founder"] = groups["dbContactChatRoom"].map(parse_founder)
            groups["chatroom"] = groups["dbContactChatRoom"].map(parse_chatroom)
            groups["table"] = "Chat_" + groups.index.map(username_to_md5)
        except Exception as err:
            logger.warning(f"Failed due to {err}")
        else:
            self.groups = groups.drop(columns=groups.filter(like="dbContact").columns)

    def get_message_table(self, message_db: Path):
        with sqlite3.connect(message_db) as conn:
            tables = pd.read_sql(
                "SELECT * FROM sqlite_sequence", conn
            ).assign(**{"message_db": message_db})
        return tables

    def get_message_by_id(self, id, contact):
        database = contact.at[id, "message_db"]
        table = contact.at[id, "table"]
        sql = f"""
            SELECT *
            FROM {table}
            WHERE CreateTime >= {self.st} AND CreateTime < {self.et}
        """
        try:
            with sqlite3.connect(database) as conn:
                msg = pd.read_sql(sql, conn)
        except:
            msg = pd.DataFrame()
        return msg

    def get_message(self):
        logger.debug("Reading messages")
        message_db_paths = self.wxfile["relativePath"].str.extract(f"(^Documents/{self.mymd5}/DB/message_\d+.sqlite$)")[0].dropna()
        message_tables = pd.concat([
            self.get_message_table(self.get_file_by_path(db_path))
            for db_path in message_db_paths
        ], axis=0)
        friends = self.friends.join(message_tables.set_index("name"), on="table")
        groups = self.groups.join(message_tables.set_index("name"), on="table")

        msg_friend = pd.concat({i: self.get_message_by_id(i, friends) for i, r in friends.iterrows()})
        msg_group = pd.concat({i: self.get_message_by_id(i, groups) for i, r in groups.iterrows()})
        msg_friend["dt"] = msg_friend["CreateTime"].map(datetime.fromtimestamp)
        msg_group["dt"] = msg_group["CreateTime"].map(datetime.fromtimestamp)
        msg_self = pd.concat([msg_friend[msg_friend["Des"]==0], msg_group[msg_group["Des"]==0]])
        return msg_self, msg_friend, msg_group

    def get_session(self):
        try:
            session_db = self.get_file_by_path(f"Documents/{self.mymd5}/session/session.db")
            with sqlite3.connect(session_db) as conn:
                sessions = pd.read_sql(f"""
                    SELECT * FROM SessionAbstract
                    WHERE CreateTime >= {self.st}
                """, conn)
        except:
            sessions = pd.DataFrame()
        return sessions


    def output_data(self, workdir):
        self.prepare_wxfile()
        self.prepare_myself_info()
    
        outfile = workdir / f"{self.myid}.json"
        if outfile.is_file():
            logger.debug("Stats data existed. Skip calculating")
            output = json.loads(outfile.read_text())
            return output
        
        self.prepare_contact()
        msg_self, msg_friend, msg_group = self.get_message()

        # 0. days
        active_days = pd.concat([msg_group["dt"], msg_friend["dt"]]).dt.normalize().nunique()
        
        # 1. sessions
        sessions = self.get_session()
        session_count = len(sessions)
        session_count_group = int(sessions["UsrName"].str.contains("@chatroom").sum())
        session_count_friend = int(sessions["UsrName"].isin(self.friends.index).sum())

        # 2. self messages
        message_count = len(msg_self)
        message_word_count = int(
            msg_self[msg_self["Type"]==1]["Message"].str.strip().str.len().sum() +
            msg_self[msg_self["Message"].str.contains("refermsg")]["Message"].apply(parse_xml_msg, path="appmsg/title").str.len().sum()
        )

        # 3. emojis
        msg_self_emoji = msg_self[msg_self["Type"]==47]["Message"].rename_axis(["chat", "idx"]).reset_index()
        msg_self_emoji["md5"] = msg_self_emoji["Message"].apply(parse_xml_msg, path="emoji", attr="md5")
        msg_self_emoji["url"] = msg_self_emoji["Message"].apply(parse_xml_msg, path="emoji", attr="cdnurl")
        emoji_url = msg_self_emoji[["md5", "url"]].dropna().drop_duplicates("md5").set_index("md5")
        msg_self_emoji_rank = (
            msg_self_emoji.groupby("md5").agg({"Message": "count", "chat": "nunique"})
                        .sort_values(["Message", "chat"], ascending=False)
                        .join(emoji_url, how="inner").head(10)
        )
        
        # 4. new friends
        friends_new = msg_friend[(msg_friend["Type"]==10000)&(msg_friend["MesSvrID"]==0)&(~msg_friend["Message"].str.contains("\""))]
        groups_new = msg_group[(msg_group["Type"]==10002)&(msg_group["Message"].str.contains("username.*others"))]
        connections_new = groups_new.droplevel(1).join(self.groups)["chatroom"].apply(lambda x: pd.read_xml(x, xpath="Member")["UserName"].to_list()).explode().nunique()

        # 5. friends
        msg_friend_agg = msg_friend.groupby(level=0).agg({
            "Message": "count", "dt": lambda s: s.dt.normalize().nunique()
        }).sort_values(["dt", "Message"], ascending=False).head(20).join(self.friends)

        # 6. groups
        msg_group_agg = msg_group.groupby(level=0)["Des"].value_counts().unstack().rename(columns={0: "send", 1: "receive"}).fillna(0)
        msg_group_agg["messages"] = msg_group_agg.sum(axis=1)
        msg_group_agg = msg_group_agg.sort_values(["messages", "send"], ascending=False).head(10).join(self.groups)

        # 7. clips
        msg_strip = msg_friend["Message"].str.replace("(\[.{2,4}\])|\s+", "", regex=True)
        msg_len = msg_strip.str.len()
        msg_clip = msg_strip[(msg_friend["Type"]==1)&(msg_len>8)&(msg_len<18)&(msg_friend["Des"]>0)]

        output = {
            "myself": {
                "id": self.myid,
                "name": self.myname,
                "headimg": self.myheadimg,
                "active_days": active_days,
                "session": {
                    "count": session_count,
                    "count_friend": session_count_friend,
                    "pct_friend": session_count_friend / session_count,
                    "count_group": session_count_group,
                    "pct_group": session_count_group / session_count,
                },
                "message": {
                    "count": message_count,
                    "count_group": len(msg_group[msg_group["Des"]==0]),
                    "count_friend": len(msg_friend[msg_friend["Des"]==0]),
                    "word_count": message_word_count
                },
                "emoji": [
                    {
                        "count": int(msg_self_emoji_rank["Message"].iat[i]),
                        "chat": int(msg_self_emoji_rank["chat"].iat[i]),
                        "url": msg_self_emoji_rank["url"].iat[i],
                    }
                    for i in range(4)
                ]
            },
            "friend": {
                "new": {
                    "count": len(friends_new)
                },
                "top": [
                    {
                        "name": msg_friend_agg["nickname"].iat[i],
                        "headimg": msg_friend_agg["headimg"].iat[i],
                        "days": int(msg_friend_agg["dt"].iat[i]),
                        "messages": int(msg_friend_agg["Message"].iat[i])
                    }
                    for i in range(5)
                ]
            },
            "group": {
                "new": {
                    "count": len(groups_new),
                    "connection": connections_new
                },
                "top": [
                    {
                        "name": msg_group_agg["nickname"].iat[i],
                        "messages": int(msg_group_agg["messages"].iat[i]),
                        "send": int(msg_group_agg["send"].iat[i])
                    }
                    for i in range(5)
                ]
            },
            "message_clip": msg_clip.sample(4).to_list(),
        }

        with open(outfile, "w", encoding="utf-8") as fp:
            json.dump(output, fp, indent=4)
        return output


def run_report():
    logger.info("Reading WeChat data")
    workdir = Path(__file__).parent.absolute()
    logger.debug(f"Working directory: {workdir}")
    wx = WeChat()
    data = wx.output_data(workdir)
    logger.info("Generating run_report")
    static_folder = workdir / "static"
    envrionment = Environment(loader=FileSystemLoader(static_folder), trim_blocks=True, lstrip_blocks=True)
    template = envrionment.get_template("template.html")
    with (workdir / "index.html").open("w", encoding="utf-8") as fp:
        fp.write(template.render(data))
    
    logger.info("Done! Scan the QR code to read your run_report")
    url = "http://{}:8000".format(get_lan_ip())
    qr = segno.make(url, version=2)
    qr_png = workdir / "qrcode.png"
    qr.save(qr_png.as_posix(), scale=10)
    webbrowser.open(qr_png.as_uri())
    logger.info("When finished, press <Ctrl+C> to exit")

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, directory=workdir, **kwargs)
        def log_message(self, format, *args) -> None:
            pass
    
    httpd = http.server.HTTPServer(("", 8000), Handler)
    try:
        httpd.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        httpd.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-log", "--loglevel", default="info")
    args = parser.parse_args()

    logging.basicConfig(format='[%(levelname)s - %(asctime)s] %(message)s')
    logger = logging.getLogger()
    logger.setLevel(args.loglevel.upper())
    
    run_report()