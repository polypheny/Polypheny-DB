import argparse
import json
import os
import random
import shutil
import tarfile
from abc import ABC, abstractmethod
from collections.abc import Callable
from gzip import GzipFile
from typing import Any

import xmltodict

"""
To rebuild the dataset follow the steps described below:

1)
Download the data:
    - https://metabrainz.org/datasets/postgres-dumps#musicbrainz-json
    - https://data.discogs.com/
    - https://github.com/marko-knoebl/chinook-database-json

2)
Dump all raw dataset files into a folder named polypheny_demo like this:
polypheny_demo/
├── relational/
│   ├── *.json
├── graph/
│   ├── *.tar.xz
├── document/
│   ├── *.xml.gz

Files do not need to be uncompressed.

3)
Then run the script with
```
python utils.py --src <path>/polypheny_demo --dst <output_path>
```
"""


def clean_str(s: str) -> str:
    mappings = str.maketrans({"*": "", "+": "", "@": "", ";": "", "#": "", "-": ""})
    return s.translate(mappings)

class Dataset(ABC):
    src_dir: str
    dst_dir: str
    artists: set[str]
    name: str

    def __init__(self, src_dir: str, dst_dir: str, name: str):
        self.src_dir = src_dir
        self.dst_dir = dst_dir
        self.name = name

        if not os.path.exists(self.dst_dir):
            os.mkdir(self.dst_dir)

        self.artists = set()

    def _iterate_file(self, file_name: str, f: Callable[[dict], Any]):
        path = f"{self.src_dir}/{file_name}"
        print(f"Loading file {path}")

        if path.endswith(".json"):
            with open(path, "r") as file:
                j = json.load(file)
                for line in j:
                    f(line)

        elif path.endswith(".tar.xz"):
            with tarfile.open(path, "r:xz") as tar:
                for member in tar.getmembers():
                    print(member)
                    if member.name.startswith("mbdump"):
                        t = tar.extractfile(member)
                        if t is None:
                            return
                        for line in t:
                            j = json.loads(line)
                            f(j)

        elif path.endswith(".xml.gz"):
            def handle(_, j):
                f(j)
                return True
            xmltodict.parse(GzipFile(path), item_depth=2, item_callback=handle)

    def _create_file(self, file: str):
        open(file, "x").close()

    def mv_files(self, files: tuple[str, str] | list[tuple[str, str]]):
        if type(files) is tuple:
            shutil.copy(files[0], files[1])
        else:
            for file in files:
                shutil.copy(file[0], file[1])

    @abstractmethod
    def create_metafile(self):
        ...

    @abstractmethod
    def expand_artists(self, artists: set[str]) -> set[str]:
        ...

    @abstractmethod
    def create_reduced_files(self):
        ...


class Chinook(Dataset):
    ALBUM: str = "Album.json"
    ARTIST: str = "Artist.json"
    GENRE: str = "Genre.json"
    MEDIATYPE: str = "MediaType.json"
    TRACK: str = "Track.json"

    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/relational"
        dst_dir = f"{dst_dir}/relational"

        super().__init__(src_dir, dst_dir, "CHINOOK")

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            self.artists.add(j["Name"])

        super()._iterate_file(self.ARTIST, handle)

        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__CHINOOK__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        return self.artists

    def create_reduced_files(self):
        files = [(f"{self.src_dir}/{file_name}", f"{self.dst_dir}/{file_name}") for file_name in os.listdir(self.src_dir)]
        self.mv_files(files)

class Musicbrainz(Dataset):
    AREA: str = "area.tar.xz"
    ARTIST: str = "artist.tar.xz"
    EVENT: str = "event.tar.xz"
    INSTRUMENT: str = "instrument.tar.xz"
    LABEL: str = "label.tar.xz"
    PLACE: str = "place.tar.xz"
    RECORDING: str = "recording.tar.xz"
    RELEASE: str = "release.tar.xz"
    RELEASE_GROUP: str = "release-group.tar.xz"
    SERIES: str = "series.tar.xz"
    WORK: str = "work.tar.xz"

    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/graph"
        dst_dir = f"{dst_dir}/graph"
        self.top_artists = set()
        super().__init__(src_dir, dst_dir, "MUSICBRAINZ")

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            if "name" in j:
                name = j["name"]
                self.artists.add(name)
                if j["rating"]["votes-count"] > 20:
                    self.top_artists.add(name)

        super()._iterate_file(self.ARTIST, handle)

        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__MUSICBRAINZ__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        self.artists = artists.union(self.top_artists)
        return self.artists

    def create_reduced_files(self):
        # Reduction of ARTIST file
        artists = []
        def reduce_top_artists(j: dict, artists=artists):
            if j["name"] in self.artists:
                artists.append(f"{clean_str(json.dumps(j))}\n")
        super()._iterate_file(self.ARTIST, reduce_top_artists)
        with open(f"{self.dst_dir}/artists.jsonl", "w") as file:
            file.writelines(artists)

        # Reduction of RELEASE file
        releases = []
        def reduce_releases(j: dict, artists=artists, releases=releases):
            if j["artist-credit"][0]["name"] in artists:
                releases.append(f"{clean_str(json.dumps(j))}\n")
        super()._iterate_file(self.RELEASE, reduce_releases)
        with open(f"{self.dst_dir}/releases.jsonl", "w") as file:
            file.writelines(releases)


class Discogs(Dataset):
    ARTIST: str = "discogs_20260801_artists.xml.gz"
    LABELS: str = "discogs_20260801_labels.xml.gz"
    MASTERS: str = "discogs_20260801_masters.xml.gz"
    RELEASES: str = "discogs_20260801_releases.xml.gz"

    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/document"
        dst_dir = f"{dst_dir}/document"
        super().__init__(src_dir, dst_dir, "DISCOGS")

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            if "name" in j:
                self.artists.add(j["name"])

        super()._iterate_file(self.ARTIST, handle)

        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__DISCOGS__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        random_artists = set(random.sample(list(self.artists), 100))
        self.artists = artists.union(random_artists)
        return self.artists

    def create_reduced_files(self):
        artists = []
        def reduce_top_artists(j: dict, artists=artists):
            if "name" in j and j["name"] in self.artists:
                artists.append(f"{clean_str(json.dumps(j))}\n")
        super()._iterate_file(self.ARTIST, reduce_top_artists)
        with open(f"{self.dst_dir}/artists.jsonl", "w") as file:
            file.writelines(artists)

        masters = []
        def reduce_masters(j: dict, artists=artists):
            artists = j["artists"]["artist"]
            artists = artists if type(artists) is list else [artists]
            for artist in artists:
                if artist["name"] in self.artists:
                    masters.append(f"{clean_str(json.dumps(j))}\n")
                    break
        super()._iterate_file(self.MASTERS, reduce_masters)
        with open(f"{self.dst_dir}/masters.jsonl", "w") as file:
            file.writelines(masters)

        label_names = []
        releases = []
        def reduce_releases(j: dict, artists=artists, label_names=label_names):
            artists = j["artists"]["artist"]
            artists = artists if type(artists) is list else [artists]
            for artist in artists:
                if artist["name"] in self.artists:
                    releases.append(f"{clean_str(json.dumps(j))}\n")
                    break
            labels = j["labels"]["label"]
            labels = labels if type(labels) is list else [labels]
            for label in labels:
                label_names.append(label["@name"])
        super()._iterate_file(self.RELEASES, reduce_releases)
        with open(f"{self.dst_dir}/releases.jsonl", "w") as file:
            file.writelines(releases)

        labels = []
        def reduce_labels(j: dict, label_names=label_names):
            if "name" in j and j["name"] in label_names:
                labels.append(f"{clean_str(json.dumps(j))}\n")
        super()._iterate_file(self.LABELS, reduce_labels)
        with open(f"{self.dst_dir}/labels.jsonl", "w") as file:
            file.writelines(labels)


def create_checkpoint(name: str, *datasets: Dataset):
    data = {}
    for dataset in datasets:
        data[dataset.name] = list(dataset.artists)
    with open(name, "w") as checkpoint_file:
        json.dump(data, checkpoint_file)

def load_checkpoint(name: str, src_dir: str, dst_dir: str) -> tuple[Chinook, Musicbrainz, Discogs] | None:
    datasets = (Chinook(src_dir, dst_dir), Musicbrainz(src_dir, dst_dir), Discogs(src_dir, dst_dir))

    if not os.path.exists(name):
        return None

    with open(name, "r") as checkpoint_file:
        data = json.load(checkpoint_file)
        datasets[0].artists = set(data["CHINOOK"])
        datasets[1].artists = set(data["MUSICBRAINZ"])
        datasets[2].artists = set(data["DISCOGS"])

    return datasets



def parse_datasets(src_dir: str, dst_dir: str):
    checkpoint = load_checkpoint("checkpoint_artists", src_dir, dst_dir)
    if checkpoint is not None:
        RELATIONAL, GRAPH, DOCUMENT = checkpoint
    else:
        RELATIONAL = Chinook(src_dir, dst_dir)
        GRAPH = Musicbrainz(src_dir, dst_dir)
        DOCUMENT = Discogs(src_dir, dst_dir)

        relational_artists = RELATIONAL.load_artists()
        graph_artists = GRAPH.load_artists()
        document_artists = DOCUMENT.load_artists()

        common_artists: set[str] = relational_artists.intersection(graph_artists, document_artists)

        relational_artists = RELATIONAL.expand_artists(common_artists)
        graph_artists = GRAPH.expand_artists(common_artists)
        document_artists = DOCUMENT.expand_artists(common_artists)

        print(f"Total artists (Relational: {len(relational_artists)}, Graph: {len(graph_artists)}, Document: {len(document_artists)})")
        create_checkpoint("checkpoint_artists", RELATIONAL, GRAPH, DOCUMENT)

    #RELATIONAL.create_reduced_files()
    #GRAPH.create_reduced_files()
    DOCUMENT.create_reduced_files()

    RELATIONAL.create_metafile()
    GRAPH.create_metafile()
    DOCUMENT.create_metafile()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", type=str, required=False)
    parser.add_argument("--dst-dir", type=str, required=False, default="./src/main/resources")

    args = parser.parse_args()

    parse_datasets(args.src_dir, args.dst_dir)
