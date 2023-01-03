package com.flipkart.neo.solr.ltr.banner.cache;

import java.util.Set;

import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;

public interface BannerCache extends Iterable<IndexedBanner> {
  IndexedBanner getBanner(String bannerId);
  void putBanner(IndexedBanner banner);
  void putBannerIfAbsent(IndexedBanner banner);
  void deleteBanner(String bannerId);

  /**
   * Asking for set here as time complexity of retainAll is directly proportion to
   * .contain() complexity of the input collection.
   * @param bannerIds set of bannerIds to be retained.
   */
  void retainAllBanners(Set<String> bannerIds);

  int size();
}
