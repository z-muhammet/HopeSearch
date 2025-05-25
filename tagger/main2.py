import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from HopeSearch.tagger.src.storage.mongo_context import MongoDbContext
from src.tasks.scheduler import main
mongo = MongoDbContext()

if __name__ == "__main__":
    main()
