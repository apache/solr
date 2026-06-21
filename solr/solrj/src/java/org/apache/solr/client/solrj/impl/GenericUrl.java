package org.apache.solr.client.solrj.impl;

import com.google.api.client.http.UrlEncodedParser;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.Key;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.escape.CharEscapers;
import com.google.api.client.util.escape.Escaper;
import com.google.api.client.util.escape.PercentEscaper;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * URL builder in which the query parameters are specified as generic data key/value pairs, based on
 * the specification <a href="http://tools.ietf.org/html/rfc3986">RFC 3986: Uniform Resource
 * Identifier (URI)</a>.
 *
 * <p>The query parameters are specified with the data key name as the parameter name, and the data
 * value as the parameter value. Subclasses can declare fields for known query parameters using the
 * {@link Key} annotation. {@code null} parameter names are not allowed, but {@code null} query
 * values are allowed.
 *
 * <p>Query parameter values are parsed using {@link UrlEncodedParser#parse(String, Object)}.
 *
 * <p>Implementation is not thread-safe.
 *
 * @since 1.0
 * @author Yaniv Inbar
 */
public class GenericUrl extends GenericData {

  private static final Escaper URI_FRAGMENT_ESCAPER = new PercentEscaper("=&-_.!~*'()@:$,;/?:");
  private String query;

  /** Scheme (lowercase), for example {@code "https"}. */
  private String scheme;

  /** Host, for example {@code "www.google.com"}. */
  private String host;

  /** User info or {@code null} for none, for example {@code "username:password"}. */
  private String userInfo;

  /** Port number or {@code -1} if undefined, for example {@code 443}. */
  private int port = -1;

  /**
   * Decoded path component by parts with each part separated by a {@code '/'} or {@code null} for
   * none, for example {@code "/m8/feeds/contacts/default/full"} is represented by {@code "", "m8",
   * "feeds", "contacts", "default", "full"}.
   *
   * <p>Use {@link #appendRawPath(String)} to append to the path, which ensures that no extra slash
   * is added.
   */
  private List<String> pathParts;

  /** Fragment component or {@code null} for none. */
  private String fragment;

  /**
   * If true, the URL string originally given is used as is (without encoding, decoding and
   * escaping) whenever referenced; otherwise, part of the URL string may be encoded or decoded as
   * deemed appropriate or necessary.
   */
  private boolean verbatim;

  public GenericUrl() {}

  /**
   * Constructs a GenericUrl from a URL encoded string.
   *
   * <p>Any known query parameters with pre-defined fields as data keys will be parsed based on
   * their data type. Any unrecognized query parameter will always be parsed as a string.
   *
   * <p>Any {@link MalformedURLException} is wrapped in an {@link IllegalArgumentException}.
   *
   * <p>Upgrade warning: starting in version 1.18 this parses the encodedUrl using new
   * URL(encodedUrl). In previous versions it used new URI(encodedUrl). In particular, this means
   * that only a limited set of schemes are allowed such as "http" and "https", but that parsing is
   * compliant with, at least, RFC 3986.
   *
   * @param encodedUrl encoded URL, including any existing query parameters that should be parsed
   * @throws IllegalArgumentException if the URL has a syntax error
   */
  public GenericUrl(String encodedUrl) {
    this(encodedUrl, false);
  }

  /**
   * Constructs a GenericUrl from a string.
   *
   * <p>Any known query parameters with pre-defined fields as data keys will be parsed based on
   * their data type. Any unrecognized query parameter will always be parsed as a string.
   *
   * <p>Any {@link MalformedURLException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @param encodedUrl encoded URL, including any existing query parameters that should be parsed
   * @param verbatim flag, to specify if URL should be used as is (without encoding, decoding and
   *     escaping)
   * @throws IllegalArgumentException if URL has a syntax error
   */
  public GenericUrl(String encodedUrl, boolean verbatim) {
    this(parseURL(encodedUrl), verbatim);
  }

  /**
   * Constructs from a URI.
   *
   * @param uri URI
   * @since 1.14
   */
  public GenericUrl(URI uri) {
    this(uri, false);
  }

  /**
   * Constructs from a URI.
   *
   * @param uri URI
   * @param verbatim flag, to specify if URL should be used as is (without encoding, decoding and
   *     escaping)
   */
  public GenericUrl(URI uri, boolean verbatim) {
    this(
        uri.getScheme(),
        uri.getHost(),
        uri.getPort(),
        uri.getRawPath(),
        uri.getRawFragment(),
        uri.getRawQuery(),
        uri.getRawUserInfo(),
        verbatim);
  }

  /**
   * Constructs from a URL.
   *
   * @param url URL
   * @since 1.14
   */
  public GenericUrl(URL url) {
    this(url, false);
  }

  /**
   * Constructs from a URL.
   *
   * @param url URL
   * @param verbatim flag, to specify if URL should be used as is (without encoding, decoding and
   *     escaping)
   * @since 1.14
   */
  public GenericUrl(URL url, boolean verbatim) {
    this(
        url.getProtocol(),
        url.getHost(),
        url.getPort(),
        url.getPath(),
        url.getRef(),
        url.getQuery(),
        url.getUserInfo(),
        verbatim);
  }

  private GenericUrl(
      String scheme,
      String host,
      int port,
      String path,
      String fragment,
      String query,
      String userInfo,
      boolean verbatim) {
    this.query = query;
    this.scheme = scheme.toLowerCase(Locale.US);
    this.host = host;
    this.port = port;
    this.pathParts = toPathParts(path, verbatim);
    this.verbatim = verbatim;
    if (verbatim) {
      this.fragment = fragment;
      if (query != null) {
        UrlEncodedParser.parse(query, this, false);
      }
      this.userInfo = userInfo;
    } else {
      this.fragment = fragment != null ? CharEscapers.decodeUri(fragment) : null;
      if (query != null) {
        UrlEncodedParser.parse(query, this);
      }
      this.userInfo = userInfo != null ? CharEscapers.decodeUri(userInfo) : null;
    }
  }

  public String getQuery() {
    return query;
  }

  @Override
  public int hashCode() {
    return build().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj) || !(obj instanceof GenericUrl)) {
      return false;
    }
    GenericUrl other = (GenericUrl) obj;
    return build().equals(other.build());
  }

  @Override
  public String toString() {
    return build();
  }

  @Override
  public GenericUrl clone() {
    GenericUrl result = (GenericUrl) super.clone();
    if (pathParts != null) {
      result.pathParts = new ArrayList<String>(pathParts);
    }
    return result;
  }

  @Override
  public GenericUrl set(String fieldName, Object value) {
    return (GenericUrl) super.set(fieldName, value);
  }

  /**
   * Returns the scheme (lowercase), for example {@code "https"}.
   *
   * @since 1.5
   */
  public final String getScheme() {
    return scheme;
  }

  /**
   * Sets the scheme (lowercase), for example {@code "https"}.
   *
   * @since 1.5
   */
  public final void setScheme(String scheme) {
    this.scheme = Preconditions.checkNotNull(scheme);
  }

  /**
   * Returns the host, for example {@code "www.google.com"}.
   *
   * @since 1.5
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the host, for example {@code "www.google.com"}.
   *
   * @since 1.5
   */
  public final void setHost(String host) {
    this.host = Preconditions.checkNotNull(host);
  }

  /**
   * Returns the user info or {@code null} for none, for example {@code "username:password"}.
   *
   * @since 1.15
   */
  public final String getUserInfo() {
    return userInfo;
  }

  /**
   * Sets the user info or {@code null} for none, for example {@code "username:password"}.
   *
   * @since 1.15
   */
  public final void setUserInfo(String userInfo) {
    this.userInfo = userInfo;
  }

  /**
   * Returns the port number or {@code -1} if undefined, for example {@code 443}.
   *
   * @since 1.5
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the port number, for example {@code 443}.
   *
   * @since 1.5
   */
  public final void setPort(int port) {
    Preconditions.checkArgument(port >= -1, "expected port >= -1");
    this.port = port;
  }

  /**
   * Returns the decoded path component by parts with each part separated by a {@code '/'} or {@code
   * null} for none.
   *
   * @since 1.5
   */
  public List<String> getPathParts() {
    return pathParts;
  }

  /**
   * Sets the decoded path component by parts with each part separated by a {@code '/'} or {@code
   * null} for none.
   *
   * <p>For example {@code "/m8/feeds/contacts/default/full"} is represented by {@code "", "m8",
   * "feeds", "contacts", "default", "full"}.
   *
   * <p>Use {@link #appendRawPath(String)} to append to the path, which ensures that no extra slash
   * is added.
   *
   * @since 1.5
   */
  public void setPathParts(List<String> pathParts) {
    this.pathParts = pathParts;
  }

  /**
   * Returns the fragment component or {@code null} for none.
   *
   * @since 1.5
   */
  public String getFragment() {
    return fragment;
  }

  /**
   * Sets the fragment component or {@code null} for none.
   *
   * @since 1.5
   */
  public final void setFragment(String fragment) {
    this.fragment = fragment;
  }

  /**
   * Constructs the string representation of the URL, including the path specified by {@link
   * #pathParts} and the query parameters specified by this generic URL.
   */
  public final String build() {
    return buildAuthority() + buildRelativeUrl();
  }

  /**
   * Constructs the portion of the URL containing the scheme, host and port.
   *
   * <p>For the URL {@code "http://example.com/something?action=add"} this method would return
   * {@code "http://example.com"}.
   *
   * @return scheme://[user-info@]host[:port]
   * @since 1.9
   */
  public final String buildAuthority() {
    // scheme, [user info], host, [port]
    StringBuilder buf = new StringBuilder();
    buf.append(Preconditions.checkNotNull(scheme));
    buf.append("://");
    if (userInfo != null) {
      buf.append(verbatim ? userInfo : CharEscapers.escapeUriUserInfo(userInfo)).append('@');
    }
    buf.append(Preconditions.checkNotNull(host));
    int port = this.port;
    if (port != -1) {
      buf.append(':').append(port);
    }
    return buf.toString();
  }

  /**
   * Constructs the portion of the URL beginning at the rooted path.
   *
   * <p>For the URL {@code "http://example.com/something?action=add"} this method would return
   * {@code "/something?action=add"}.
   *
   * @return path with with leading '/' if the path is non-empty, query parameters and fragment
   * @since 1.9
   */
  public final String buildRelativeUrl() {
    StringBuilder buf = new StringBuilder();
    if (pathParts != null) {
      appendRawPathFromParts(buf);
    }
    addQueryParams(entrySet(), buf, verbatim);

    // URL fragment
    String fragment = this.fragment;
    if (fragment != null) {
      buf.append('#').append(verbatim ? fragment : URI_FRAGMENT_ESCAPER.escape(fragment));
    }
    return buf.toString();
  }

  /**
   * Constructs the URI based on the string representation of the URL from {@link #build()}.
   *
   * <p>Any {@link URISyntaxException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @return new URI instance
   * @since 1.14
   */
  public final URI toURI() {
    return toURI(build());
  }

  /**
   * Constructs the URL based on the string representation of the URL from {@link #build()}.
   *
   * <p>Any {@link MalformedURLException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @return new URL instance
   * @since 1.14
   */
  public final URL toURL() {
    return parseURL(build());
  }

  /**
   * Constructs the URL based on {@link URL#URL(URL, String)} with this URL representation from
   * {@link #toURL()} and a relative url.
   *
   * <p>Any {@link MalformedURLException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @return new URL instance
   * @since 1.14
   */
  public final URL toURL(String relativeUrl) {
    try {
      URL url = toURL();
      return new URL(url, relativeUrl);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Returns the first query parameter value for the given query parameter name.
   *
   * @param name query parameter name
   * @return first query parameter value
   */
  public Object getFirst(String name) {
    Object value = get(name);
    if (value instanceof Collection<?>) {
      @SuppressWarnings("unchecked")
      Collection<Object> collectionValue = (Collection<Object>) value;
      Iterator<Object> iterator = collectionValue.iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }
    return value;
  }

  /**
   * Returns all query parameter values for the given query parameter name.
   *
   * @param name query parameter name
   * @return unmodifiable collection of query parameter values (possibly empty)
   */
  public Collection<Object> getAll(String name) {
    Object value = get(name);
    if (value == null) {
      return Collections.emptySet();
    }
    if (value instanceof Collection<?>) {
      @SuppressWarnings("unchecked")
      Collection<Object> collectionValue = (Collection<Object>) value;
      return Collections.unmodifiableCollection(collectionValue);
    }
    return Collections.singleton(value);
  }

  /**
   * Returns the raw encoded path computed from the {@link #pathParts}.
   *
   * @return raw encoded path computed from the {@link #pathParts} or {@code null} if {@link
   *     #pathParts} is {@code null}
   */
  public String getRawPath() {
    List<String> pathParts = this.pathParts;
    if (pathParts == null) {
      return null;
    }
    StringBuilder buf = new StringBuilder();
    appendRawPathFromParts(buf);
    return buf.toString();
  }

  /**
   * Sets the {@link #pathParts} from the given raw encoded path.
   *
   * @param encodedPath raw encoded path or {@code null} to set {@link #pathParts} to {@code null}
   */
  public void setRawPath(String encodedPath) {
    pathParts = toPathParts(encodedPath, verbatim);
  }

  /**
   * Appends the given raw encoded path to the current {@link #pathParts}, setting field only if it
   * is {@code null} or empty.
   *
   * <p>The last part of the {@link #pathParts} is merged with the first part of the path parts
   * computed from the given encoded path. Thus, if the current raw encoded path is {@code "a"}, and
   * the given encoded path is {@code "b"}, then the resulting raw encoded path is {@code "ab"}.
   *
   * @param encodedPath raw encoded path or {@code null} to ignore
   */
  public void appendRawPath(String encodedPath) {
    if (encodedPath != null && encodedPath.length() != 0) {
      List<String> appendedPathParts = toPathParts(encodedPath, verbatim);
      if (pathParts == null || pathParts.isEmpty()) {
        this.pathParts = appendedPathParts;
      } else {
        int size = pathParts.size();
        pathParts.set(size - 1, pathParts.get(size - 1) + appendedPathParts.get(0));
        pathParts.addAll(appendedPathParts.subList(1, appendedPathParts.size()));
      }
    }
  }
  /**
   * Returns the decoded path parts for the given encoded path.
   *
   * @param encodedPath slash-prefixed encoded path, for example {@code
   *     "/m8/feeds/contacts/default/full"}
   * @return decoded path parts, with each part assumed to be preceded by a {@code '/'}, for example
   *     {@code "", "m8", "feeds", "contacts", "default", "full"}, or {@code null} for {@code null}
   *     or {@code ""} input
   */
  public static List<String> toPathParts(String encodedPath) {
    return toPathParts(encodedPath, false);
  }

  /**
   * Returns the path parts (decoded if not {@code verbatim}).
   *
   * @param encodedPath slash-prefixed encoded path, for example {@code
   *     "/m8/feeds/contacts/default/full"}
   * @param verbatim flag, to specify if URL should be used as is (without encoding, decoding and
   *     escaping)
   * @return path parts (decoded if not {@code verbatim}), with each part assumed to be preceded by
   *     a {@code '/'}, for example {@code "", "m8", "feeds", "contacts", "default", "full"}, or
   *     {@code null} for {@code null} or {@code ""} input
   */
  public static List<String> toPathParts(String encodedPath, boolean verbatim) {
    if (encodedPath == null || encodedPath.length() == 0) {
      return null;
    }
    List<String> result = new ArrayList<String>();
    int cur = 0;
    boolean notDone = true;
    while (notDone) {
      int slash = encodedPath.indexOf('/', cur);
      notDone = slash != -1;
      String sub;
      if (notDone) {
        sub = encodedPath.substring(cur, slash);
      } else {
        sub = encodedPath.substring(cur);
      }
      result.add(verbatim ? sub : CharEscapers.decodeUriPath(sub));
      cur = slash + 1;
    }
    return result;
  }

  private void appendRawPathFromParts(StringBuilder buf) {
    int size = pathParts.size();
    for (int i = 0; i < size; i++) {
      String pathPart = pathParts.get(i);
      if (i != 0) {
        buf.append('/');
      }
      if (pathPart.length() != 0) {
        buf.append(verbatim ? pathPart : CharEscapers.escapeUriPath(pathPart));
      }
    }
  }

  /** Adds query parameters from the provided entrySet into the buffer. */
  static void addQueryParams(
      Set<Entry<String, Object>> entrySet, StringBuilder buf, boolean verbatim) {
    // (similar to UrlEncodedContent)
    boolean first = true;
    for (Map.Entry<String, Object> nameValueEntry : entrySet) {
      Object value = nameValueEntry.getValue();
      if (value != null) {
        String name =
            verbatim
                ? nameValueEntry.getKey()
                : CharEscapers.escapeUriQuery(nameValueEntry.getKey());
        if (value instanceof Collection<?>) {
          Collection<?> collectionValue = (Collection<?>) value;
          for (Object repeatedValue : collectionValue) {
            first = appendParam(first, buf, name, repeatedValue, verbatim);
          }
        } else {
          first = appendParam(first, buf, name, value, verbatim);
        }
      }
    }
  }

  private static boolean appendParam(
      boolean first, StringBuilder buf, String name, Object value, boolean verbatim) {
    if (first) {
      first = false;
      buf.append('?');
    } else {
      buf.append('&');
    }
    buf.append(name);
    String stringValue =
        verbatim ? value.toString() : CharEscapers.escapeUriQuery(value.toString());
    if (stringValue.length() != 0) {
      buf.append('=').append(stringValue);
    }
    return first;
  }

  /**
   * Returns the URI for the given encoded URL.
   *
   * <p>Any {@link URISyntaxException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @param encodedUrl encoded URL
   * @return URI
   */
  private static URI toURI(String encodedUrl) {
    try {
      return new URI(encodedUrl);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Returns the URI for the given encoded URL.
   *
   * <p>Any {@link MalformedURLException} is wrapped in an {@link IllegalArgumentException}.
   *
   * @param encodedUrl encoded URL
   * @return URL
   */
  private static URL parseURL(String encodedUrl) {
    try {
      return new URL(encodedUrl);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}