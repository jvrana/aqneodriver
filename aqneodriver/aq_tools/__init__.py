from .queries import QueryAquariumInventory
from .queries import QueryAquariumSamples

aq_samples_to_cypher = QueryAquariumSamples().__call__
aq_inventory_to_cypher = QueryAquariumInventory().__call__
# aq_jobs_to_cypher = QueryAquariumJobs().__call__

__all__ = ["aq_samples_to_cypher", "aq_inventory_to_cypher"]
