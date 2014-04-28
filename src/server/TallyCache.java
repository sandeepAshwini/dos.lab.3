package server;

import java.util.HashMap;
import java.util.Map;

import base.NationCategories;
import base.OlympicException;
import base.Tally;

public class TallyCache {
	private Map<NationCategories, Tally> tallyCache;
	private Map<NationCategories, Long> timeStamps;
	private static long TTL = 30000;

	public TallyCache() {
		this.tallyCache = new HashMap<NationCategories, Tally>();
		this.timeStamps = new HashMap<NationCategories, Long>();
	}

	private boolean isCacheHit(NationCategories nation) {
		if (tallyCache.containsKey(nation)) {
			return true;
		}
		return false;
	}

	public Tally getTally(NationCategories nation) throws OlympicException {
		if (isCacheHit(nation)) {
			synchronized (this.tallyCache) {
				return this.tallyCache.get(nation);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public Tally getTally(NationCategories nation, long currentTimeStamp)
			throws OlympicException {
		if (isCacheHit(nation)) {
			synchronized (this.timeStamps) {
				if (currentTimeStamp > this.timeStamps.get(nation) + TTL) {
					throw new OlympicException("Cached tally is stale.");
				}
			}
			synchronized (this.tallyCache) {
				return this.tallyCache.get(nation);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public void cache(NationCategories nation, Tally medalTally) {
		synchronized (this.tallyCache) {
			this.tallyCache.put(nation, medalTally);
		}
	}

	public void cache(NationCategories nation, Tally medalTally, long timestamp) {
		synchronized (this.tallyCache) {
			this.tallyCache.put(nation, medalTally);
			this.timeStamps.put(nation, timestamp);
		}
	}

	public void invalidateEntry(NationCategories nation) {
		synchronized (this.tallyCache) {
			this.tallyCache.remove(nation);

			synchronized (this.timeStamps) {
				if (this.timeStamps.containsKey(nation)) {
					this.timeStamps.remove(nation);
				}
			}

		}
	}
}
