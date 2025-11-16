import csv
def load_movies_from_file_tags(filepath: str = "tags.csv"):
    tags = []
    with open(filepath, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=",")
        for row in reader:
            tag = Tag(
                userId=int(row["userId"]),
                movieId=int(row["movieId"]),
                tag=row["tag"],
                timestamp=int(row["timestamp"])
            )
            tags.append(tag.__dict__)
    return tags
class Tag:
    def __init__(self, userId: int, movieId: int, tag: str, timestamp: int):
        self.userId = userId
        self.movieId = movieId
        self.tag = tag
        self.timestamp = timestamp
print(load_movies_from_file_tags())