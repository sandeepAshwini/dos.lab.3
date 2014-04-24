package server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import base.Athlete;
import base.EventCategories;
import base.OlympicException;
import base.Results;

public class ScoreCache {
	private Map<EventCategories, List<Athlete>> scoreCache;
	private Map<EventCategories, Long> timeStamps;
	private static long TTL = 10000;

	public ScoreCache() {
		this.scoreCache = new HashMap<EventCategories, List<Athlete>>();
		this.timeStamps = new HashMap<EventCategories, Long>();
	}

	private boolean isCacheHit(EventCategories eventName) {
		if (scoreCache.containsKey(eventName)) {
			return true;
		}
		return false;
	}

	public List<Athlete> getScores(EventCategories eventName)
			throws OlympicException {
		if (isCacheHit(eventName)) {
			synchronized (this.scoreCache) {
				return this.scoreCache.get(eventName);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public List<Athlete> getScores(EventCategories eventName, long currentTime)
			throws OlympicException {
		if (isCacheHit(eventName)) {
			synchronized (this.timeStamps) {
				if (currentTime > this.timeStamps.get(eventName) + TTL) {
					throw new OlympicException("Cache entry is stale.");
				}
			}
			synchronized (this.scoreCache) {
				return this.scoreCache.get(eventName);
			}
		} else {
			throw new OlympicException("Not in cache.");
		}
	}

	public void cache(EventCategories eventName, List<Athlete> scores) {
		synchronized (this.scoreCache) {
			this.scoreCache.put(eventName, scores);
		}
	}

	public void cache(EventCategories eventName, List<Athlete> scores,
			long timestamp) {
		synchronized (this.scoreCache) {
			this.scoreCache.put(eventName, scores);
			this.timeStamps.put(eventName, timestamp);
		}
	}

	public void invalidateEntry(EventCategories eventName) {
		synchronized (this.scoreCache) {
			this.scoreCache.remove(eventName);
		}
		synchronized (this.timeStamps) {
			if (this.timeStamps.containsKey(eventName)) {
				this.timeStamps.remove(eventName);
			}

		}

	}
}
