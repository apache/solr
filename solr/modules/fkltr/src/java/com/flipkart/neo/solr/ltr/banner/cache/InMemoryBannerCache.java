package com.flipkart.neo.solr.ltr.banner.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;

public class InMemoryBannerCache implements BannerCache {
  private final Map<String, IndexedBanner> map;

  public InMemoryBannerCache(int initialSize) {
    map = new ConcurrentHashMap<>(initialSize);
  }

  @Override
  public IndexedBanner getBanner(String bannerId) {
    return map.get(bannerId);
  }

  @Override
  public void putBanner(IndexedBanner banner) {
    map.put(banner.getId(), banner);
  }

  @Override
  public void putBannerIfAbsent(IndexedBanner banner) {
    map.putIfAbsent(banner.getId(), banner);
  }

  @Override
  public void deleteBanner(String bannerId) {
    map.remove(bannerId);
  }

  @Override
  public void retainAllBanners(Set<String> bannerIds) {
    map.keySet().retainAll(bannerIds);
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public Iterator<IndexedBanner> iterator() {
    return map.values().iterator();
  }
}
