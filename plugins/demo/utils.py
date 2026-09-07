import argparse
from gzip import GzipFile
import json
import xmltodict

def parse_musicbrainz(src: str, dst: str):
        top_artists = []
        relations = []
        with open(src, "r") as json_file:
            for line in json_file:
                data = json.loads(line)
                if data["rating"]["votes-count"] > 20:
                    data.pop("aliases")
                    data.pop("disambiguation")
                    data.pop("sort-name")
                    for relation in data.pop("relations"):
                        relations.append(f"{json.dumps(relation)}\n")
                    top_artists.append(f"{json.dumps(data)}\n")

        with open(dst, "w") as json_file:
            json_file.writelines(top_artists)

        with open("relations.json", "w") as json_file:
            json_file.writelines(relations)

def parse_discogs(src: str, dst: str):
    with open(dst, "w") as file:
        def handle_xml(_, j):
            if "id" in j.keys():
                j["id"] = int(j["id"])
            file.write(f"{json.dumps(j)}\n")
            return True

        xmltodict.parse(GzipFile(src), item_depth=2, item_callback=handle_xml)

def get_dict_keys(src: str):
    with open(src, "r") as file:
        line = file.readline()
        j = json.loads(line)
        print(j.keys())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--musicbrainz", action="store_true", required=False)
    parser.add_argument("--discogs", action="store_true", required=False)
    parser.add_argument("--get-keys", action="store_true", required=False)

    parser.add_argument("--src", type=str, required=False)
    parser.add_argument("--dst", type=str, required=False)

    args = parser.parse_args()

    if args.musicbrainz:
        if args.get_keys:
            get_dict_keys(args.src)
        else:
            parse_musicbrainz(args.src, args.dst)

    if args.discogs:
        if args.get_keys:
            get_dict_keys(args.src)
        else:
            parse_discogs(args.src, args.dst)
