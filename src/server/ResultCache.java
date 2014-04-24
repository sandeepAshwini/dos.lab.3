package server;

import java.util.HashMap;
import java.util.Map;

import base.EventCategories;
import base.OlympicException;
import base.Results;

public class ResultCache {
	private Map<EventCategories, Results> resultCache;
	private Map<EventCategories, Long> timeStamps;
	private static long TTL = 100000;

	public ResultCache() {
		this.resultCache = new HashMap<EventCategories, Results>();
		this.timeStamps = new HashMap<EventCategories, Long>();
	}

	private boolean isCacheHit(EventCategories eventName) {
		if (resultCache.containsKey(eventName)) {
			return true;
		}
		return false;
	}

	public Results getResults(EventCategories eventName)
			throws OlympicException {
		if (isCacheHit(eventName)) {
			synchronized (this.resultCache) {
				return this.resultCache.get(eventName);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public Results getResults(EventCategories eventName, long currentTimeStamp)
			throws OlympicException {
		if (isCacheHit(eventName)) {
			synchronized (this.timeStamps) {
				if (currentTimeStamp > this.timeStamps.get(eventName) + TTL) {
					throw new OlympicException("Cached results are stale.");
				}
			}
			synchronized (this.resultCache) {
				return this.resultCache.get(eventName);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public void cache(EventCategories eventName, Results result) {
		synchronized (this.resultCache) {
			this.resultCache.put(eventName, result);
		}
	}

	public void cache(EventCategories eventName, Results result, long timestamp) {
		synchronized (this.resultCache) {
			this.resultCache.put(eventName, result);
			this.timeStamps.put(eventName, timestamp);
		}
	}

	public void invalidateEntry(EventCategories eventName) {
		synchronized (this.resultCache) {
			this.resultCache.remove(eventName);
		}
		synchronized (this.timeStamps) {
			if (this.timeStamps.containsKey(eventName)) {
				this.timeStamps.remove(eventName);
			}

		}
	}

}
