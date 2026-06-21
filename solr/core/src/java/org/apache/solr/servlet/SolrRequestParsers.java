/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.servlet;

import javax.servlet.MultipartConfigElement;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Part;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.security.Principal;
import java.util.*;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.RequestHandlers;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.tracing.GlobalTracer;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.PATH;


public class SolrRequestParsers {

  private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static class SolrRequestParsersHolder {
    public static SolrRequestParsers HOLDER_INSTANCE = new SolrRequestParsers();
  }

  public static SolrRequestParsers getInstance() {
    return SolrRequestParsersHolder.HOLDER_INSTANCE;
  }

  public static class DefaultSolrRequestParsersHolder {
    public static SolrRequestParsers HOLDER_INSTANCE = new SolrRequestParsers();;
  }

  public static SolrRequestParsers getDefaultInstance() {
    return DefaultSolrRequestParsersHolder.HOLDER_INSTANCE;
  }


  public static class IncompatibleSolrException extends SolrException {
    public IncompatibleSolrException() {
      super(ErrorCode.SERVER_ERROR, "Solr requires that request parameters sent using application/x-www-form-urlencoded " + "content-type can be read through the request input stream. Unfortunately, the "
          + "stream was empty / not available. This may be caused by another servlet filter calling " + "ServletRequest.getParameter*() before SolrDispatchFilter, please remove it.");
    }
  }

  // Should these constants be in a more public place?
  public static final String MULTIPART = "multipart";
  public static final String FORMDATA = "formdata";
  public static final String RAW = "raw";
  public static final String SIMPLE = "simple";
  public static final String STANDARD = "standard";

  private final static Charset CHARSET_US_ASCII = Charset.forName("US-ASCII");

  public static final String INPUT_ENCODING_KEY = "ie";
  private final static byte[] INPUT_ENCODING_BYTES = INPUT_ENCODING_KEY.getBytes(CHARSET_US_ASCII);
 // private final ByteBuffer INPUT_ENCODING_BUFFER =  ByteBuffer.wrap(INPUT_ENCODING_BYTES);

  public final static String REQUEST_TIMER_SERVLET_ATTRIBUTE = "org.apache.solr.RequestTimer";

  // Multipart config used to enable Jetty's multipart/form-data parsing on demand (see parse()).
  // temp dir = default, maxFileSize = unlimited, maxRequestSize = unlimited, fileSizeThreshold = 2MB.
  private static final MultipartConfigElement MULTIPART_CONFIG =
      new MultipartConfigElement(null, -1L, -1L, 2097152);

  //private final HashMap<String,SolrRequestParser> parsers = new HashMap<>();
  private final boolean enableRemoteStreams;
  private final boolean enableStreamBody;
 // private StandardRequestParser standard;
  private boolean handleSelect = true;
  private boolean addHttpRequestToContext;

  /**
   * Pass in an xml configuration.  A null configuration will enable
   * everything with maximum values.
   */
  public SolrRequestParsers(SolrConfig globalConfig) {
    final int multipartUploadLimitKB, formUploadLimitKB;
    if (globalConfig == null) {
      multipartUploadLimitKB = formUploadLimitKB = Integer.MAX_VALUE;
      enableRemoteStreams = true;
      enableStreamBody = false;
      handleSelect = true;
      addHttpRequestToContext = false;
    } else {
      multipartUploadLimitKB = globalConfig.getMultipartUploadLimitKB();

      formUploadLimitKB = globalConfig.getFormUploadLimitKB();

      enableRemoteStreams = globalConfig.isEnableRemoteStreams();
      enableStreamBody = globalConfig.isEnableStreamBody();

      // Let this filter take care of /select?xxx format
      handleSelect = globalConfig.isHandleSelect();

      addHttpRequestToContext = globalConfig.isAddHttpRequestToContext();
    }
    init(multipartUploadLimitKB, formUploadLimitKB);
  }

  private SolrRequestParsers() {
    enableRemoteStreams = false;
    enableStreamBody = false;
    handleSelect = false;
    addHttpRequestToContext = false;
    init(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

  private void init(int multipartUploadLimitKB, int formUploadLimitKB) {
  //  MultipartRequestParser multi = new MultipartRequestParser(multipartUploadLimitKB);
   // RawRequestParser raw = new RawRequestParser();
  //  FormDataRequestParser formdata = new FormDataRequestParser(formUploadLimitKB);
   // standard = new StandardRequestParser(multi, raw, formdata);

    // I don't see a need to have this publicly configured just yet
    // adding it is trivial
   // parsers.put(MULTIPART, multi);
   // parsers.put(FORMDATA, formdata);
  //  parsers.put(RAW, raw);
//    parsers.put(SIMPLE, new SimpleRequestParser());
  //  parsers.put(STANDARD, standard);
 //   parsers.put("", standard);
  }

  private static RTimerTree getRequestTimer(HttpServletRequest req) {
    final Object reqTimer = req.getAttribute(REQUEST_TIMER_SERVLET_ATTRIBUTE);
    if (reqTimer instanceof RTimerTree) {
      return ((RTimerTree) reqTimer);
    }

    return new RTimerTree();
  }

  public SolrQueryRequest parse(SolrCore core, String path, HttpServletRequest req, SolrParams solrParams) throws Exception {
 //   SolrRequestParser parser = standard;

    // TODO -- in the future, we could pick a different parser based on the request

    // Pick the parser from the request...
    ArrayList<ContentStream> streams = new ArrayList<>(1);
  //  SolrParams params = parser.parseParamsAndFillStreams(req, streams);
    if (GlobalTracer.get().tracing()) {
 //     GlobalTracer.get().getTracer().activeSpan().setTag("params", params.toString());
    }

    String contentType = req.getContentType();

    // Normalize "type/subtype; charset=..." down to the bare media type for comparison.
    String baseType = contentType;
    if (baseType != null) {
      int sep = baseType.indexOf(';');
      if (sep > 0) {
        baseType = baseType.substring(0, sep);
      }
      baseType = baseType.trim();
    }

    HttpRequestContentStream servletStream = null;
    if ("multipart/form-data".equalsIgnoreCase(baseType)) {
      // multipart/form-data: simple form fields are request parameters, the file parts are content
      // streams. (Mirrors the upstream MultipartRequestParser.) Adding the raw multipart body as a
      // single content stream instead would reach content-stream handlers (e.g. UpdateRequestHandler)
      // as the unsupported "multipart/form-data" type and 415.
      // Tell Jetty dynamically that we want multipart processing (otherwise getParts() throws
      // "No multipart config for servlet").
      req.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, MULTIPART_CONFIG);
      Map<String,String[]> fieldParams = new HashMap<>();
      for (Part part : req.getParts()) {
        if (part.getSubmittedFileName() == null) { // form field, not a file upload
          String value;
          try (InputStream in = part.getInputStream()) {
            value = new String(in.readAllBytes(), StandardCharsets.UTF_8);
          }
          MultiMapSolrParams.addParam(part.getName().trim(), value, fieldParams);
        } else { // file upload -> content stream
          streams.add(new PartContentStream(part));
        }
      }
      if (!fieldParams.isEmpty()) {
        // Field parts are the primary params; fall back to anything already on the query string.
        solrParams = SolrParams.wrapDefaults(new MultiMapSolrParams(fieldParams), solrParams);
      }
    } else if ("application/x-www-form-urlencoded".equalsIgnoreCase(baseType)) {
      // form-urlencoded body is purely request parameters; the servlet container has already parsed
      // it into the parameter map (see HttpSolrCall, which builds solrParams from getParameterMap()).
      // Do NOT add it as a content stream -- it would otherwise reach content-stream handlers
      // (e.g. UpdateRequestHandler for commit/optimize/delete) as the unsupported
      // "application/x-www-form-urlencoded" type and 415. (Mirrors the upstream FormDataRequestParser.)
    } else {
      servletStream = new HttpRequestContentStream(req, contentType);
      // Only treat the HTTP request body as a content stream when the request actually carries one.
      // A GET/HEAD/DELETE has no body; adding an empty stream here causes content stream handlers
      // (e.g. UpdateRequestHandler) to fail with "Missing ContentType" when the content is instead
      // supplied via stream.body / stream.url / stream.file request parameters.
      if (contentType != null || req.getContentLengthLong() > 0) {
        streams.add(servletStream);
      }
    }
    SolrQueryRequest sreq = buildRequestFrom(core, solrParams, streams, getRequestTimer(req), req);

    // wt selects the RESPONSE writer, not the request body format. Only fall back to it to label the
    // request body when the client sent no Content-Type at all; otherwise an XML/JSON update body sent
    // with wt=javabin (a common response default) would be mislabeled as javabin and fail to parse
    // ("Invalid version or the data in not in 'javabin' format").
    String wt = sreq.getParams().get("wt");
    if (servletStream != null && contentType == null && wt != null && wt.equals("javabin")) {
      servletStream.setContentType("application/javabin");
    }

    // Handlers and login will want to know the path. If it contains a ':'
    // the handler could use it for RESTful URLs
    sreq.getContext().put(PATH, RequestHandlers.normalize(path));
    sreq.getContext().put("httpMethod", req.getMethod());

    if (addHttpRequestToContext) {
      sreq.getContext().put("httpRequest", req);
    }
    return sreq;
  }

  public SolrQueryRequest buildRequestFrom(SolrCore core, SolrParams params, Collection<ContentStream> streams) throws Exception {
    return buildRequestFrom(core, params, streams, new RTimerTree(), null);
  }

  private SolrQueryRequest buildRequestFrom(SolrCore core, SolrParams params, Collection<ContentStream> streams, RTimerTree requestTimer, final HttpServletRequest req) throws Exception {
    if (params == null) {
      params = new MultiMapSolrParams(new java.util.HashMap<>());
    }
    // The content type will be applied to all streaming content
    String contentType = params.get(CommonParams.STREAM_CONTENTTYPE);

    // Handle anything with a remoteURL
    String[] strs = params.getParams(CommonParams.STREAM_URL);
    if (strs != null) {
      if (!enableRemoteStreams) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Remote Streaming is disabled.");
      }
      for (final String url : strs) {
        ContentStreamBase stream = new ContentStreamBase.URLStream(new URL(url));
        if (contentType != null) {
          stream.setContentType(contentType);
        }
        streams.add(stream);
      }
    }

    // Handle streaming files
    strs = params.getParams(CommonParams.STREAM_FILE);
    if (strs != null) {
      if (!enableRemoteStreams) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Remote Streaming is disabled. See http://lucene.apache.org/solr/guide/requestdispatcher-in-solrconfig.html for help");
      }
      for (final String file : strs) {
        ContentStreamBase stream = new ContentStreamBase.FileStream(new File(file));
        if (contentType != null) {
          stream.setContentType(contentType);
        }
        streams.add(stream);
      }
    }

    // Check for streams in the request parameters
    strs = params.getParams(CommonParams.STREAM_BODY);
    if (strs != null) {
      if (!enableStreamBody) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Stream Body is disabled. See http://lucene.apache.org/solr/guide/requestdispatcher-in-solrconfig.html for help");
      }
      for (final String body : strs) {
        ContentStreamBase stream = new ContentStreamBase.StringStream(body);
        if (contentType != null) {
          stream.setContentType(contentType);
        }
        streams.add(stream);
      }
    }

    final SolrCall httpSolrCall = req == null ? null : (SolrCall) req.getAttribute(HttpSolrCall.class.getName());
//    FormDataRequestParser.SQPSolrQueryRequestBase q = new FormDataRequestParser.SQPSolrQueryRequestBase(core, params, requestTimer, req, httpSolrCall);
//    if (streams != null && streams.size() > 0) {
//      q.setContentStreams(streams);
//    }

    SolrQueryRequestBase q = new MySolrQueryRequestBase(req, httpSolrCall, core, params);
    q.setContentStreams(streams);

    return q;
  }

  private static SolrCall getHttpSolrCall(HttpServletRequest req) {
    return req == null ? null : (SolrCall) req.getAttribute(HttpSolrCall.class.getName());
  }

  /**
   * Given a url-encoded query string (UTF-8), map it into solr params
   */
  public static MultiMapSolrParams parseQueryString(String queryString) {
    Map<String,String[]> map = new Object2ObjectArrayMap<>(8);
    parseQueryString(queryString, map);
    return new MultiMapSolrParams(map);
  }

  public static boolean isFormData(HttpServletRequest req) {
    String contentType = req.getContentType();
    if (contentType != null) {
      int idx = contentType.indexOf(';');
      if (idx > 0) { // remove the charset definition "; charset=utf-8"
        contentType = contentType.substring(0, idx);
      }
      contentType = contentType.trim();
      if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
        return true;
      }
    }
    return false;
  }
  /**
   * Given a url-encoded query string (UTF-8), map it into the given map
   *
   * @param queryString as given from URL
   * @param map         place all parameters in this map
   */
  static void parseQueryString(final String queryString, final Map<String,String[]> map) {
    if (queryString != null && queryString.length() > 0) {
      try {
        final int len = queryString.length();
        // this input stream emulates to get the raw bytes from the URL as passed to servlet container, it disallows any byte > 127 and enforces to %-escape them:
        final InputStream in = new MyInputStream(len, queryString);
        parseFormDataContent(in, Long.MAX_VALUE, StandardCharsets.UTF_8, map, true);
      } catch (IOException ioe) {
        throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
      }
    }
  }

  /**
   * Given a url-encoded form from POST content (as InputStream), map it into the given map.
   * The given InputStream should be buffered!
   *
   * @param postContent to be parsed
   * @param charset     to be used to decode resulting bytes after %-decoding
   * @param map         place all parameters in this map
   */
  static long parseFormDataContent(final InputStream postContent, final long maxLen, Charset charset, final Map<String,String[]> map,
      boolean supportCharsetParam) throws IOException {
    CharsetDecoder charsetDecoder = supportCharsetParam ? null : getCharsetDecoder(charset);
    final LinkedList<Object> buffer = supportCharsetParam ? new LinkedList<>() : null;
    long len = 0L, keyPos = 0L, valuePos = 0L;
    final ByteArrayOutputStream keyStream = new ByteArrayOutputStream(), valueStream = new ByteArrayOutputStream();
    ByteArrayOutputStream currentStream = keyStream;
    for (; ; ) {
      int b = postContent.read();
      switch (b) {
        case -1: // end of stream
        case '&': // separator
          if (keyStream.size() > 0) {
            final byte[] keyBytes = keyStream.toByteArray(), valueBytes = valueStream.toByteArray();
            if (Arrays.equals(keyBytes, INPUT_ENCODING_BYTES)) {
              // we found a charset declaration in the raw bytes
              if (charsetDecoder != null) {
                throw new SolrException(ErrorCode.BAD_REQUEST, supportCharsetParam ?
                    ("Query string invalid: duplicate '" + INPUT_ENCODING_KEY + "' (input encoding) key.") :
                    ("Key '" + INPUT_ENCODING_KEY + "' (input encoding) cannot " + "be used in POSTed application/x-www-form-urlencoded form data. "
                        + "To set the input encoding of POSTed form data, use the " + "'Content-Type' header and provide a charset!"));
              }
              // decode the charset from raw bytes
              charset = Charset.forName(decodeChars(valueBytes, keyPos, getCharsetDecoder(CHARSET_US_ASCII)));
              charsetDecoder = getCharsetDecoder(charset);
              // finally decode all buffered tokens
              decode(buffer, map, charsetDecoder);
            } else if (charsetDecoder == null) {
              // we have no charset decoder until now, buffer the keys / values for later processing:
              buffer.add(keyBytes);
              buffer.add(keyPos);
              buffer.add(valueBytes);
              buffer.add(valuePos);
            } else {
              // we already have a charsetDecoder, so we can directly decode without buffering:
              final String key = decodeChars(keyBytes, keyPos, charsetDecoder), value = decodeChars(valueBytes, valuePos, charsetDecoder);
              MultiMapSolrParams.addParam(key.trim(), value, map);
            }
          } else if (valueStream.size() > 0) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded invalid: missing key");
          }
          keyStream.reset();
          valueStream.reset();
          keyPos = valuePos = len + 1;
          currentStream = keyStream;
          break;
        case '+': // space replacement
          currentStream.write(' ');
          break;
        case '%': // escape
          final int upper = digit16(b = postContent.read());
          len++;
          final int lower = digit16(b = postContent.read());
          len++;
          currentStream.write(((upper << 4) + lower));
          break;
        case '=': // kv separator
          if (currentStream == keyStream) {
            valuePos = len + 1;
            currentStream = valueStream;
            break;
          }
          // fall-through
        default:
          currentStream.write(b);
      }
      if (b == -1) {
        break;
      }
      len++;
      if (len > maxLen) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content exceeds upload limit of " + (maxLen / 1024L) + " KB");
      }
    }
    // if we have not seen a charset declaration, decode the buffer now using the default one (UTF-8 or given via Content-Type):
    if (buffer != null && !buffer.isEmpty()) {
      assert charsetDecoder == null;
      decode(buffer, map, getCharsetDecoder(charset));
    }
    return len;
  }

//  static long parseFormDataContent(final InputStream postContent, final long maxLen, Charset charset, final Map<String,String[]> map,
//      boolean supportCharsetParam) throws IOException {
//    CharsetDecoder charsetDecoder = supportCharsetParam ? null : getCharsetDecoder(charset);
//    //  final LinkedList<Object> buffer = supportCharsetParam ? new LinkedList<>() : null;
//    long len = 0L, keyPos = 0L, valuePos = 0L;
//
//    MutableDirectBuffer eb1 = ExpandableBuffers.getInstance().acquire(-1, true); //ExpandableBuffers.buffer1.get();
//    ExpandableDirectBufferOutputStream keyStream = new ExpandableDirectBufferOutputStream(eb1);
//    MutableDirectBuffer eb2 =  ExpandableBuffers.getInstance().acquire(-1, true);//new ExpandableDirectByteBuffer(4096);//ExpandableBuffers.buffer2.get();
//    ExpandableDirectBufferOutputStream valueStream = new ExpandableDirectBufferOutputStream(eb2);
//    if (charsetDecoder == null) {
//      charsetDecoder = getCharsetDecoder(StandardCharsets.UTF_8);
//    }
//    //final ByteArrayOutputStream keyStream = new ByteArrayOutputStream(), valueStream = new ByteArrayOutputStream(
//    // );
//    ExpandableDirectBufferOutputStream currentStream = keyStream;
//    for (; ; ) {
//      int b = postContent.read();
//      switch (b) {
//        case -1: // end of stream
//        case '&': // separator
//          final ByteBuffer keyBytes = keyStream.buffer().byteBuffer().asReadOnlyBuffer(), valueBytes = valueStream.buffer().byteBuffer().asReadOnlyBuffer();
//          keyBytes.position(keyStream.offset() + keyStream.buffer().wrapAdjustment());
//          keyBytes.limit(keyStream.position() + keyStream.buffer().wrapAdjustment());
//          valueBytes.position(valueStream.offset());
//          valueBytes.limit(valueStream.position() + valueStream.buffer().wrapAdjustment());
//          // we already have a charsetDecoder, so we can directly decode without buffering:
//          final String key = decodeCharsBB(keyBytes, keyPos, charsetDecoder), value = decodeCharsBB(valueBytes, valuePos, charsetDecoder);
//          MultiMapSolrParams.addParam(key.trim(), value, map);
//
//          keyPos = valuePos = len + 1;
//          MutableDirectBuffer expandableBuffer1 = keyStream.buffer();
//          expandableBuffer1.byteBuffer().clear();
//          MutableDirectBuffer expandableBuffer2 = valueStream.buffer();
//          expandableBuffer2.byteBuffer().clear();
//          keyStream.wrap(expandableBuffer1);
//          valueStream.wrap(expandableBuffer2);
//
//          currentStream = keyStream;
//          break;
//        case '+': // space replacement
//          currentStream.write(' ');
//          break;
//        case '%': // escape
//          final int upper = digit16(b = postContent.read());
//          len++;
//          final int lower = digit16(b = postContent.read());
//          len++;
//          currentStream.write(((upper << 4) + lower));
//          break;
//        case '=': // kv separator
//          if (currentStream == keyStream) {
//            valuePos = len + 1;
//            currentStream = valueStream;
//            break;
//          }
//          // fall-through
//        default:
//          currentStream.write(b);
//      }
//      if (b == -1) {
//        break;
//      }
//      len++;
//      if (len > maxLen) {
//        throw new SolrException(ErrorCode.BAD_REQUEST, "application/x-www-form-urlencoded content exceeds upload limit of " + (maxLen / 1024L) + " KB");
//      }
//    }
//    ExpandableBuffers.getInstance().release(eb1);
//    ExpandableBuffers.getInstance().release(eb2);
//    return len;
//  }

  private static CharsetDecoder getCharsetDecoder(Charset charset) {
    return charset.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
  }

  private static String decodeChars(byte[] bytes, long position, CharsetDecoder charsetDecoder) {
    try {
      return charsetDecoder.decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException cce) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "URLDecoder: Invalid character encoding detected after position " + position + " of query string / form data (while parsing as " + charsetDecoder.charset().name() + ')');
    }
  }

  private static void decode(final LinkedList<Object> input, final Map<String,String[]> map, CharsetDecoder charsetDecoder) {
    for (final Iterator<Object> it = input.iterator(); it.hasNext(); ) {
      final byte[] keyBytes = (byte[]) it.next();
      it.remove();
      final Long keyPos = (Long) it.next();
      it.remove();
      final byte[] valueBytes = (byte[]) it.next();
      it.remove();
      final Long valuePos = (Long) it.next();
      it.remove();
      MultiMapSolrParams.addParam(decodeChars(keyBytes, keyPos, charsetDecoder).trim(), decodeChars(valueBytes, valuePos, charsetDecoder), map);
    }
  }

  private static String decodeCharsBB(ByteBuffer bytes, long position, CharsetDecoder charsetDecoder) {
    try {
      return charsetDecoder.decode(bytes).toString();
    } catch (CharacterCodingException cce) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "URLDecoder: Invalid character encoding detected after position " + position + " of query string / form data (while parsing as " + charsetDecoder.charset().name() + ')');
    }
  }

  private static void decodeBuffer(final LinkedList<Object> input, final Map<String,String[]> map, CharsetDecoder charsetDecoder) {
    for (final Iterator<Object> it = input.iterator(); it.hasNext(); ) {
      final Object keyBytes = it.next();
      it.remove();
      final Long keyPos = (Long) it.next();
      it.remove();
      final Object valueBytes = it.next();
      it.remove();
      final Long valuePos = (Long) it.next();
      it.remove();
      MultiMapSolrParams.addParam(decodeCharsBB((ByteBuffer) keyBytes, keyPos, charsetDecoder).trim(), decodeCharsBB((ByteBuffer) valueBytes, valuePos, charsetDecoder), map);
    }
  }

  private static int digit16(int b) {
    if (b == -1) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: Incomplete trailing escape (%) pattern");
    }
    if (b >= '0' && b <= '9') {
      return b - '0';
    }
    if (b >= 'A' && b <= 'F') {
      return b - ('A' - 10);
    }
    if (b >= 'a' && b <= 'f') {
      return b - ('a' - 10);
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: Invalid digit (" + ((char) b) + ") in escape (%) pattern");
  }

  public boolean isHandleSelect() {
    return handleSelect;
  }

  public void setHandleSelect(boolean handleSelect) {
    this.handleSelect = handleSelect;
  }

  public boolean isAddRequestHeadersToContext() {
    return addHttpRequestToContext;
  }

  public void setAddRequestHeadersToContext(boolean addRequestHeadersToContext) {
    this.addHttpRequestToContext = addRequestHeadersToContext;
  }

  //-----------------------------------------------------------------
  //-----------------------------------------------------------------

  // I guess we don't really even need the interface, but i'll keep it here just for kicks
  interface SolrRequestParser {
    SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams) throws Exception;
  }

  private static class MySolrQueryRequestBase extends SolrQueryRequestBase {
    private final HttpServletRequest req;
    private final SolrCall solrCall;

    public MySolrQueryRequestBase(HttpServletRequest req, SolrCall httpSolrCall, SolrCore core, SolrParams params) {
      super(core, params);
      this.req = req;
      this.solrCall = httpSolrCall;
    }

    @Override public Principal getUserPrincipal() {
      return req == null ? null : req.getUserPrincipal();
    }

    @Override public List<CommandOperation> getCommands(boolean validateInput) {
      if (solrCall != null) {
        return solrCall.getCommands(solrCall.getSolrReq(), validateInput);
      }
      return super.getCommands(validateInput);
    }

    @Override public Map<String,String> getPathTemplateValues() {
      if (solrCall instanceof V2HttpCall) {
        return ((V2HttpCall) solrCall).getUrlParts();
      }
      return super.getPathTemplateValues();
    }

    @Override public SolrCall getHttpSolrCall() {
      return solrCall;
    }
  }

  /**
   * The raw parser just uses the params directly
   */
  static class RawRequestParser implements SolrRequestParser {
    @Override public SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams) {
      streams.add(new SolrRequestParsers.HttpRequestContentStream(req, null));
      return new MultiMapSolrParams(req.getParameterMap());
    }
  }

  static class HttpRequestContentStream extends ContentStreamBase {
    private final HttpServletRequest req;

    public HttpRequestContentStream(HttpServletRequest req, String contentType) {
      this.req = req;

      this.contentType = contentType;


      // name = ???
      // sourceInfo = ???

//      String v = req.getHeader("Content-Length");
//      if (v != null) {
//        size = Long.parseLong(v);
//      }
    }

    @Override public InputStream getStream() throws IOException {
      return req.getInputStream();
    }
  }

  /**
   * Wrap a multipart {@link Part} (a file upload) as a {@link ContentStream}.
   */
  static class PartContentStream extends ContentStreamBase {
    private final Part part;

    public PartContentStream(Part part) {
      this.part = part;
      contentType = part.getContentType();
      name = part.getName();
      sourceInfo = part.getSubmittedFileName();
      size = part.getSize();
    }

    @Override public InputStream getStream() throws IOException {
      return part.getInputStream();
    }
  }
  //
  private static class MyInputStream extends InputStream {
    private final int len;
    private final String queryString;
    int pos;

    public MyInputStream(int len, String queryString) {
      this.len = len;
      this.queryString = queryString;
      pos = 0;
    }

    @Override public int read() {
      if (pos < len) {
        final char ch = queryString.charAt(pos);
        if (ch > 127) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "URLDecoder: The query string contains a not-%-escaped byte > 127 at position " + pos);
        }
        pos++;
        return ch;
      } else {
        return -1;
      }
    }
  }

  //-----------------------------------------------------------------
  //-----------------------------------------------------------------

  /**
   * The simple parser just uses the params directly, does not support POST URL-encoded forms
   */
//  static class SimpleRequestParser implements SolrRequestParser {
//    @Override public SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams) {
//      return parseQueryString(req.getQueryString());
//    }
 }

  /**
   * Wrap an HttpServletRequest as a ContentStream
   */




  /**
   * Extract Multipart streams
   */
//  static class MultipartRequestParser implements SolrRequestParser {
//    private final MultipartConfigElement multipartConfigElement;
//
//    public MultipartRequestParser(int uploadLimitKB) {
//      multipartConfigElement = new MultipartConfigElement(null, // temp dir (null=default)
//          -1, // maxFileSize  (-1=none)
//          (long) uploadLimitKB << 10, // maxRequestSize
//          2097152); // fileSizeThreshold after which will go to disk
//    }
//
//    @Override public SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams) throws Exception {
////      if (!isMultipart(req)) {
////        throw new SolrException(ErrorCode.BAD_REQUEST, "Not multipart content! " + req.getContentType());
////      }
//      // Magic way to tell Jetty dynamically we want multi-part processing.  "Request" here is a Jetty class
//      req.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, multipartConfigElement);
//
//   //   MultiMapSolrParams params = SolrRequestParsers.getInstance().parseQueryString(req.getQueryString());
//
//     MultiMapSolrParams params = new MultiMapSolrParams(req.getParameterMap());
//
//      // IMPORTANT: the Parts will all have the delete() method called by cleanupMultipartFiles()
//
//      for (Part part : req.getParts()) {
//        if (part.getSubmittedFileName() == null) { // thus a form field and not file upload
//          // If it's a form field, put it in our parameter map
//          String partAsString = org.apache.commons.io.IOUtils.toString(new PartContentStream(part).getReader());
//          MultiMapSolrParams.addParam(part.getName().trim(), partAsString, params.getMap());
//        } else { // file upload
//          streams.add(new PartContentStream(part));
//        }
//      }
//      return params;
//    }
//
//    static boolean isMultipart(HttpServletRequest req) {
//      // Jetty utilities
//      String ct = req.getContentType();
//      return ct != null && ct.startsWith("multipart/form-data");
//    }
//
//    /**
//     * Wrap a MultiPart-{@link Part} as a {@link ContentStream}
//     */
//    static class PartContentStream extends ContentStreamBase {
//      private final Part part;
//
//      public PartContentStream(Part part) {
//        this.part = part;
//        contentType = part.getContentType();
//        name = part.getName();
//        sourceInfo = part.getSubmittedFileName();
//        size = part.getSize();
//      }
//
//      @Override public InputStream getStream() throws IOException {
//        return part.getInputStream();
//      }
//    }
//  }

  /**
   * Clean up any tmp files created by MultiPartInputStream.
   */
//  static void cleanupMultipartFiles(MultiPartFormInputStream multiParts) {
//    // See Jetty MultiPartCleanerListener from which we drew inspiration
//
//    log.debug("Deleting multipart files");
//
//    multiParts.deleteParts();
//  }

  /**
   * Extract application/x-www-form-urlencoded form data for POST requests
   */
//  static class FormDataRequestParser implements SolrRequestParser {
//    private static final long WS_MASK = (1L << ' ') | (1L << '\t') | (1L << '\r') | (1L << '\n') | (1L << '#') | (1L << '/') | (0x01); // set 1 bit so 0xA0 will be flagged as possible whitespace
//
//    private final int uploadLimitKB;
//
//    public FormDataRequestParser(int limit) {
//      uploadLimitKB = limit;
//    }
//
//    public SolrParams parseParamsAndFillStreams(HttpServletRequest req, ArrayList<ContentStream> streams, InputStream in) throws Exception {
//      final Map<String,String[]> map = new Object2ObjectArrayMap<>(8);
//
//      // also add possible URL parameters and include into the map (parsed using UTF-8):
//      final String qs = req.getQueryString();
//      if (qs != null) {
//    //    SolrRequestParsers.getInstance().parseQueryString(qs, map);
//      }
//
//      // may be -1, so we check again later. But if it's already greater we can stop processing!
//      final long totalLength = req.getContentLength();
//      final long maxLength = ((long) uploadLimitKB) << 10;
//      if (totalLength > maxLength) {
//        throw new SolrException(ErrorCode.BAD_REQUEST,
//            "application/x-www-form-urlencoded content length (" + totalLength + " bytes) exceeds upload limit of " + uploadLimitKB + " KB");
//      }
//
//      // get query String from request body, using the charset given in content-type:
//      final String cs = ContentStreamBase.getCharsetFromContentType(req.getContentType());
//      final Charset charset = (cs == null) ? StandardCharsets.UTF_8 : Charset.forName(cs);
//      InputStream is = null;
//      try {
//        is = in == null ? req.getInputStream() : in;
//
//        final long bytesRead = SolrRequestParsers.parseFormDataContent(is, maxLength, charset, map, false);
//        if (bytesRead == 0L && totalLength > 0L) {
//          throw new SolrRequestParsers.IncompatibleSolrException();
//        }
//      } catch (IOException ioe) {
//        throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
//      } catch (IllegalStateException ise) {
//        throw (SolrException) new SolrRequestParsers.IncompatibleSolrException().initCause(ise);
//      }
//
//      return new MultiMapSolrParams(map);
//    }

//    @Override public SolrParams parseParamsAndFillStreams(HttpServletRequest req, ArrayList<ContentStream> streams) throws Exception {
////      if (!SolrRequestParsers.isFormData(req)) {
////        throw new SolrException(ErrorCode.BAD_REQUEST, "Not application/x-www-form-urlencoded content: " + req.getContentType());
////      }
//
//      return parseParamsAndFillStreams(req, streams, null);
//    }

 // }
  /**
   * The default Logic
   */
//  static class StandardRequestParser implements SolrRequestParsers.SolrRequestParser {
//    SolrRequestParsers.MultipartRequestParser multipart;
//    SolrRequestParsers.RawRequestParser raw;
//    SolrRequestParsers.FormDataRequestParser formdata;
//
//    StandardRequestParser(SolrRequestParsers.MultipartRequestParser multi, SolrRequestParsers.RawRequestParser raw,
//        SolrRequestParsers.FormDataRequestParser formdata) {
//      this.multipart = multi;
//      this.raw = raw;
//      this.formdata = formdata;
//    }
//
//    @Override public SolrParams parseParamsAndFillStreams(final HttpServletRequest req, ArrayList<ContentStream> streams) throws Exception {
//      String contentType = req.getContentType();
//      String method = req.getMethod(); // No need to uppercase... HTTP verbs are case sensitive
//      String uri = req.getRequestURI();
//      boolean isV2 = req.getAttribute(HttpSolrCall.class.getName()) instanceof V2HttpCall;
//      boolean isPost = "POST".equals(method);
//
//      // SOLR-6787 changed the behavior of a POST without content type.  Previously it would throw an exception,
//      // but now it will use the raw request parser.
//      /***
//       if (contentType == null && isPost) {
//       throw new SolrException(ErrorCode.UNSUPPORTED_MEDIA_TYPE, "Must specify a Content-Type header with POST requests");
//       }
//       ***/
//
//      // According to previous StandardRequestParser logic (this is a re-written version),
//      // POST was handled normally, but other methods (PUT/DELETE)
//      // were handled by the RestManager classes if the URI contained /schema or /config
//      if (!isPost) {
//        if (isV2) {
//          return raw.parseParamsAndFillStreams(req, streams);
//        }
//        if (contentType == null) {
//          return SolrRequestParsers.getInstance().parseQueryString(req.getQueryString());
//        }
//
//        // OK, we have a BODY at this point
//
//        boolean schemaRestPath = false;
//        int idx = uri.indexOf("/schema");
//        if (idx >= 0 && uri.endsWith("/schema") || uri.contains("/schema/")) {
//          schemaRestPath = true;
//        }
//
//        if (schemaRestPath) {
//          return raw.parseParamsAndFillStreams(req, streams);
//        }
//
//        if ("PUT".equals(method) || "DELETE".equals(method)) {
//          throw new SolrException(ErrorCode.BAD_REQUEST, "Unsupported method: " + method + " for request " + req);
//        }
//      }
//
//      if (SolrRequestParsers.isFormData(req)) {
//        String userAgent = req.getHeader("User-Agent");
//        boolean isCurl = userAgent != null && userAgent.startsWith("curl/");
//
//        InputStream input;
//        if (isCurl) {
//          input = FastInputStream.wrap(req.getInputStream());
//        } else {
//          input = req.getInputStream();
//        }
//
//        if (isCurl) {
//          SolrParams params = autodetect(req, streams, (FastInputStream) input);
//          if (params != null) return params;
//        }
//
//        return formdata.parseParamsAndFillStreams(req, streams, input);
//      }
//
//      if (!req.getParts().isEmpty()) {
//        return multipart.parseParamsAndFillStreams(req, streams);
//      }
//
//      // some other content-type (json, XML, csv, etc)
//      return raw.parseParamsAndFillStreams(req, streams);
//    }

   // private static final long WS_MASK = (1L << ' ') | (1L << '\t') | (1L << '\r') | (1L << '\n') | (1L << '#') | (1L << '/') | (0x01); // set 1 bit so 0xA0 will be flagged as possible whitespace

    /**
     * Returns the parameter map if a different content type was auto-detected
     */
//    private static SolrParams autodetect(HttpServletRequest req, ArrayList<ContentStream> streams, FastInputStream in) throws IOException {
//      String detectedContentType = null;
//      boolean shouldClose = true;
//
//      try {
//        in.peek();  // should cause some bytes to be read
//        byte[] arr = in.getBuffer();
//        int pos = in.getPositionInBuffer();
//        int end = in.getEndInBuffer();
//
//        for (int i = pos; i < end - 1; i++) {  // we do "end-1" because we check "arr[i+1]" sometimes in the loop body
//          int ch = arr[i];
//          boolean isWhitespace = ((WS_MASK >> ch) & 0x01) != 0 && (ch <= ' ' || ch == 0xa0);
//          if (!isWhitespace) {
//            // first non-whitespace chars
//            if (ch == '#'                         // single line comment
//                || (ch == '/' && (arr[i + 1] == '/' || arr[i + 1] == '*'))  // single line or multi-line comment
//                || (ch == '{' || ch == '[')       // start of JSON object
//            ) {
//              detectedContentType = "application/json";
//            }
//            if (ch == '<') {
//              detectedContentType = "text/xml";
//            }
//            break;
//          }
//        }
//
//        if (detectedContentType == null) {
//          shouldClose = false;
//          return null;
//        }
//
//        Long size = null;
//        String v = req.getHeader("Content-Length");
//        if (v != null) {
//          size = Long.valueOf(v);
//        }
//        streams.add(new InputStreamContentStream(in, detectedContentType, size));
//
//        final Map<String,String[]> map = new Object2ObjectArrayMap<>();
//        // also add possible URL parameters and include into the map (parsed using UTF-8):
//        final String qs = req.getQueryString();
//        if (qs != null) {
//        //  SolrRequestParsers.getInstance().parseQueryString(qs, map);
//        }
//
//        return new MultiMapSolrParams(map);
//
//      } catch (IOException ioe) {
//        throw new SolrException(ErrorCode.BAD_REQUEST, ioe);
//      } catch (IllegalStateException ise) {
//        throw (SolrException) new SolrRequestParsers.IncompatibleSolrException().initCause(ise);
//      } finally {
////        if (shouldClose) {
////          IOUtils.closeWhileHandlingException(in);
////        }
//      }
//    }


  /**
   * Wrap InputStream as a ContentStream
   */
//  static class InputStreamContentStream extends ContentStreamBase {
//    private final InputStream is;
//
//    public InputStreamContentStream(InputStream is, String detectedContentType, Long size) {
//      this.is = is;
//      this.contentType = detectedContentType;
//      this.size = size;
//    }
//
//    @Override public InputStream getStream() throws IOException {
//      return is;
//    }
//  }
//
//  static class SQPSolrQueryRequestBase extends SolrQueryRequestBase {
//    private final HttpServletRequest req;
//    private final SolrCall httpSolrCall;
//
//    public SQPSolrQueryRequestBase(SolrCore core, SolrParams params, RTimerTree requestTimer, HttpServletRequest req, SolrCall httpSolrCall) {
//      super(core, params, requestTimer);
//      this.req = req;
//      this.httpSolrCall = httpSolrCall;
//    }
//
//    @Override public Principal getUserPrincipal() {
//      return req == null ? null : req.getUserPrincipal();
//    }
//
//    @Override public List<CommandOperation> getCommands(boolean validateInput) {
//      if (httpSolrCall != null) {
//        return httpSolrCall.getCommands(httpSolrCall.getSolrReq(), validateInput);
//      }
//      return super.getCommands(validateInput);
//    }
//
//    @Override public Map<String,String> getPathTemplateValues() {
//      if (httpSolrCall != null && httpSolrCall instanceof V2HttpCall) {
//        return ((V2HttpCall) httpSolrCall).getUrlParts();
//      }
//      return super.getPathTemplateValues();
//    }
//
//    @Override public SolrCall getHttpSolrCall() {
//      return httpSolrCall;
//    }
//  }
//}

