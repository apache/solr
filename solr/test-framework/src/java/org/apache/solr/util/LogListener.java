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

package org.apache.solr.util;

import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.impl.MutableLogEvent;
import org.apache.logging.log4j.message.Message;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Helper code to listen for {@link LogEvent} messages (via a {@link Queue}) that you expect as a
 * result of the things you are testing, So you can make assertions about when a particular action
 * should/shouldn't cause Solr to produce a particular Log message
 *
 * <p><code>
 * // simplest possible usage...
 * // Listen for any erors from the SolrCore logger, and assert that there are none...
 * try (LogListener errLog = LogListener.error(SolrCore.class)) {
 *   // ... some test code ...
 *
 *   assertEquals(0, errLog.getCount());
 * }
 *
 * // You can also use a substring or regex to restrict the messages that are considered,
 * // and make assertions about the LogEvent's that match...
 * try (LogListener secWarnLog = LogListener.warn("org.apache.solr.security").substring("PKI")) {
 *   // ... some test code ...
 *
 *   // convinience method for only dealing with Message String of the LogEvent
 *   MatcherAssert.assertThat(secWarnLog.pollMessage(), containsString("hoss"));
 *   MatcherAssert.assertThat(secWarnLog.getQueue().isEmpty()); // no other WARNings matching PKI
 *
 *   // ... more test code ...
 *
 *   // More structured inspection of LogEvents...
 *   var logEvent = secWarnLog.getQueue().poll();
 *   assertNotNull(logEvent);
 *   assertEquals("xyz", logEvent.getContextData().getValue("tid")); // check the MDC data
 * }
 * </code>
 *
 * <p>Each <code>LogListener</code> captures &amp; queues matching Log events until it is {@link
 * #close()}ed. By default the Queue is bounded at a max capacity of 100. Regardless of what Queue
 * is used, if a Log event can't be queued (due to capacity limiting), or if events are still left
 * in the Queue when the listener is closed, then the {@link #close()} method will cause a test
 * failure.
 *
 * <p>Filtering methods such {@link #substring} and {@link #regex} can be used to restrict which Log
 * events are recorded.
 *
 * <p>Log messages are only recorded if they <em>strictly</em> match the Level specified, but the
 * Logger "name" specified is matched hierarchically against any child Loggers (ie: You must know
 * exactly what Log Level you are interested in, but you can capture all messages from the Loggers
 * under a java package, or from inner classes of a single Logger)
 *
 * <p><b>NOTE:</b> You Can <em>not</em> listen for ERROR messages from the root logger (<code>""
 * </code>) If they are being "muted" by {@link ErrorLogMuter}.
 */
@SuppressForbidden(reason = "We need to use log4J2 classes directly")
public final class LogListener implements Closeable, AutoCloseable {

  // far easier to use FQN for our (one) slf4j Logger then to use a FQN every time we refe to log4j2
  // Logger
  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(
          MethodHandles.lookup().lookupClass()); // nowarn_valid_logger

  private static final LoggerContext CTX = LoggerContext.getContext(false);

  /**
   * @see #createName
   */
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);

  /** generate a unique name for each instance to use in it's own lifecycle logging */
  private static String createName(final Level level) {
    return MethodHandles.lookup().lookupClass().getSimpleName()
        + "-"
        + level
        + "-"
        + ID_GEN.incrementAndGet();
  }

  /** Listens for ERROR log messages at the ROOT logger */
  public static LogListener error() {
    return error("");
  }

  /** Listens for ERROR log messages for the specified logger */
  public static LogListener error(final Class<?> logger) {
    return error(logger.getName());
  }

  /** Listens for ERROR log messages for the specified logger */
  public static LogListener error(final String logger) {
    return create(Level.ERROR, logger);
  }

  /** Listens for WARN log messages at the ROOT logger */
  public static LogListener warn() {
    return warn("");
  }

  /** Listens for WARN log messages for the specified logger */
  public static LogListener warn(final Class<?> logger) {
    return warn(logger.getName());
  }

  /** Listens for WARN log messages for the specified logger */
  public static LogListener warn(final String logger) {
    return create(Level.WARN, logger);
  }

  /** Listens for INFO log messages at the ROOT logger */
  public static LogListener info() {
    return info("");
  }

  /** Listens for INFO log messages for the specified logger */
  public static LogListener info(final Class<?> logger) {
    return info(logger.getName());
  }

  /** Listens for INFO log messages for the specified logger */
  public static LogListener info(final String logger) {
    return create(Level.INFO, logger);
  }

  /** Listens for DEBUG log messages at the ROOT logger */
  public static LogListener debug() {
    return debug("");
  }

  /** Listens for DEBUG log messages for the specified logger */
  public static LogListener debug(final Class<?> logger) {
    return debug(logger.getName());
  }

  /** Listens for DEBUG log messages for the specified logger */
  public static LogListener debug(final String logger) {
    return create(Level.DEBUG, logger);
  }

  // TODO: more factories for other levels?
  // TODO: no-arg factory variants that use "" -- simpler syntax for ROOT logger?

  private static LogListener create(final Level level, final String logger) {
    final String name = createName(level);
    log.info("Creating {} for log messages from: {}", name, logger);
    return new LogListener(name, logger, level);
  }

  private final String name;
  private final String loggerName;
  private final boolean removeLoggerConfigWhenDone;
  private final Level resetLevelWhenDone;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final MutablePredicateFilter filter;
  private final QueueAppender loggerAppender;

  private LogListener(final String name, final String loggerName, final Level level) {
    assert null != name;
    assert null != loggerName;
    assert null != level;

    this.name = name;
    this.loggerName = loggerName;

    final Configuration config = CTX.getConfiguration();

    // This may match loggerName, or it may be the 'nearest parent' if nothing is explicitly
    // configured for loggerName
    LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);

    // if loggerConfig refers to our nearest parent logger, then we don't have an existing
    // LoggerConfig for this specific loggerName
    // so we have to add one (and remember to clean it up later)
    this.removeLoggerConfigWhenDone = !loggerName.equals(loggerConfig.getName());
    if (this.removeLoggerConfigWhenDone) {
      // NOTE: By default inherit the same level as our existing "parent" level so we don't spam our
      // parent (nor hide less specific messages)
      loggerConfig = new LoggerConfig(loggerName, null, true);
      config.addLogger(loggerName, loggerConfig);
    }

    // Regardless of wether loggerConfig exactly matches loggerName, or is an ancestor, if it's
    // level is (strictly) more specific
    // then our configured level, it will be impossible to listen for the events we want - so track
    // the original level and modify as needed...
    // NOTE: can't use isMoreSpecificThan because it's not strict...
    this.resetLevelWhenDone =
        loggerConfig.getLevel().intLevel() < level.intLevel() ? loggerConfig.getLevel() : null;
    if (null != this.resetLevelWhenDone) {
      log.info(
          "{} (temporarily) increasing level of {} to {} in order to record matching logs",
          this.name,
          this.loggerName,
          level);
      loggerConfig.setLevel(level);
    }

    // Note: we don't just pass our level to addAppender, because that would only require it be "as
    // specifc"
    // we use a wrapper that requres an exact level match (and other predicates can be added to this
    // filter later)...
    this.filter = new MutablePredicateFilter(level);
    this.loggerAppender = new QueueAppender(name);

    loggerConfig.addAppender(loggerAppender, level, filter);
    CTX.updateLoggers();
  }

  @Override
  public void close() {
    if (!closed.getAndSet(true)) { // Don't muck with log4j if we accidently get a double close
      final LoggerConfig loggerConfig = CTX.getConfiguration().getLoggerConfig(loggerName);
      loggerConfig.removeAppender(loggerAppender.getName());
      if (null != resetLevelWhenDone) {
        loggerConfig.setLevel(resetLevelWhenDone);
      }
      if (removeLoggerConfigWhenDone) {
        CTX.getConfiguration().removeLogger(loggerName);
      }
      if (log.isInfoEnabled()) {
        log.info("Closing {} after recording {} log messages", this.name, getCount());
      }
      assertEquals(
          this.name
              + " processed log events that it could not record beause queue capacity was exceeded",
          0,
          loggerAppender.getNumCapacityExceeded());
      assertEquals(
          this.name
              + " recorded log events which were not expected & removed from the queue by the test code",
          0,
          loggerAppender.getQueue().size());
    }
  }

  /**
   * Changes the queue that will be used to record any future events that match
   *
   * @see #getQueue
   */
  public LogListener setQueue(Queue<LogEvent> queue) {
    loggerAppender.setQueue(queue);
    return this;
  }

  /**
   * Modifies this listener to filter the log events that are recorded to events that match the
   * specified substring.
   *
   * <p>Log events are considered a match if their input matches either the message String, or the
   * <code>toString</code> of an included {@link Throwable}, or any of the recursive {@link
   * Throwable#getCause}es of an included <code>Throwable</code>.
   *
   * <p>At most one filtering method may be used
   */
  public LogListener substring(final String substr) {
    setPredicate(
        (str) -> {
          return str.contains(substr);
        });
    return this;
  }

  /**
   * Modifies this listener to filter the log events that are recorded to events that match the
   * specified regex.
   *
   * <p>Log events are considered a match if their input matches either the message String, or the
   * <code>toString</code> of an included {@link Throwable}, or any of the recursive {@link
   * Throwable#getCause}es of an included <code>Throwable</code>.
   *
   * <p>At most one filtering method may be used
   */
  public LogListener regex(final Pattern pat) {
    setPredicate(
        (str) -> {
          return pat.matcher(str).find();
        });
    return this;
  }

  /**
   * Modifies this listener to filter the log events that are recorded to events that match the
   * specified regex.
   *
   * <p>Log events are considered a match if their input matches either the message String, or the
   * <code>toString</code> of an included {@link Throwable}, or any of the recursive {@link
   * Throwable#getCause}es of an included <code>Throwable</code>.
   *
   * <p>At most one filtering method may be used
   */
  public LogListener regex(final String regex) {
    return regex(Pattern.compile(regex));
  }

  /**
   * @see #regex
   * @see #substring
   * @see MutablePredicateFilter#predicate
   */
  private void setPredicate(Predicate<String> predicate) {
    if (!this.filter.predicate.compareAndSet((Predicate<String>) null, predicate)) {
      throw new IllegalStateException(
          "At most one method to set a message predicate can be called on a LogListener");
    }
  }

  /**
   * Direct access to the Queue of Log events that have been recorded, for {@link Queue#poll}ing
   * messages or any other inspection/manipulation.
   *
   * <p>If a Log event is ever processed but can not be added to this queue (because {@link
   * Queue#offer} returns false) then the {@link #close} method of this listener will fail the test.
   *
   * @see #setQueue
   * @see #pollMessage
   */
  public Queue<LogEvent> getQueue() {
    return loggerAppender.getQueue();
  }

  /**
   * Convinience method for tests that want to assert things about the (formated) message string at
   * the head of the queue, w/o needing to know/call methods on the underlying {@link LogEvent}
   * class.
   *
   * @return the formatted message string of head of the queue, or null if the queue was empty when
   *     polled.
   * @see #getQueue
   */
  public String pollMessage() {
    final LogEvent event = getQueue().poll();
    return null == event ? null : event.getMessage().getFormattedMessage();
  }

  /**
   * The total number of Log events so far processed by this instance, regardless of wether they
   * have already been removed from the queue, or if they could not be added to the queue due to
   * capacity restrictions.
   */
  public int getCount() {
    return loggerAppender.getCount();
  }

  /**
   * A Filter registered with the specified Logger to restrict what Log events we process in our
   * {@link QueueAppender}
   */
  @SuppressForbidden(reason = "We need to use log4J2 classes directly")
  private static final class MutablePredicateFilter extends AbstractFilter {
    // TODO: could probably refactor to share some code with ErrorLogMuter

    // This could probably be implemented with a combination of "LevelMatchFilter" and
    // "ConjunctionFilter" if "ConjunctionFilter" existed. Since it doesn't, we write our own more
    // specialized impl instead of writing & combining multiple generalized versions

    // may be mutated in main thread while background thread is actively logging
    public final AtomicReference<Predicate<String>> predicate =
        new AtomicReference<Predicate<String>>(null);

    final Level level;

    public MutablePredicateFilter(final Level level) {
      super(Filter.Result.ACCEPT, Filter.Result.DENY);
      assert null != level;
      this.level = level;
    }

    // NOTE: This is inspired by log4j's RegexFilter, but with an eye to being more "garbage-free"
    // friendly. Oddly, StringMatchFilter does things differently and acts like it needs to (re?)
    // format msgs when params are provided. Since RegexFilter has tests, and StringMatchFilter
    // doesn't, we assume RegexFilter knows what it's doing...

    /**
     * The main logic of our filter: Evaluate predicate against log msg &amp; throwable msg(s) if
     * and only if Level is correct; else mismatch
     */
    private Filter.Result doFilter(final Level level, final String msg, final Throwable throwable) {
      if (level.equals(this.level)) {
        final Predicate<String> pred = predicate.get(); // read from reference once
        if (null == pred) {
          return getOnMatch();
        } else {
          if (null != msg && pred.test(msg)) {
            return getOnMatch();
          }
          for (Throwable t = throwable; null != t; t = t.getCause()) {
            if (pred.test(t.toString())) {
              return getOnMatch();
            }
          }
        }
      }
      return getOnMismatch();
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg, Object... params) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, String msg, Object p0) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger, Level level, Marker marker, String msg, Object p0, Object p1) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger, Level level, Marker marker, String msg, Object p0, Object p1, Object p2) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7,
        Object p8) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(
        Logger logger,
        Level level,
        Marker marker,
        String msg,
        Object p0,
        Object p1,
        Object p2,
        Object p3,
        Object p4,
        Object p5,
        Object p6,
        Object p7,
        Object p8,
        Object p9) {
      return doFilter(level, msg, null);
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Object msg, Throwable t) {
      return doFilter(level, null == msg ? null : msg.toString(), t);
    }

    @Override
    public Result filter(Logger logger, Level level, Marker marker, Message msg, Throwable t) {
      return doFilter(level, msg.getFormattedMessage(), t);
    }

    @Override
    public Result filter(LogEvent event) {
      // NOTE: For our usage, we're not worried about needing to filter LogEvents rom remote JVMs
      // with ThrowableProxy
      // stand ins for Throwabls that don't exist in our classloader...
      return doFilter(
          event.getLevel(), event.getMessage().getFormattedMessage(), event.getThrown());
    }
  }

  /**
   * An Appender registered with the specified Logger to record the Log events that match our {@link
   * MutablePredicateFilter}
   *
   * <p>By default, we use a BlockingQueue with a capacity=100
   */
  @SuppressForbidden(reason = "We need to use log4J2 classes directly")
  private static final class QueueAppender extends AbstractAppender {

    // may be mutated in main thread while background thread is actively logging
    private final AtomicReference<Queue<LogEvent>> queue =
        new AtomicReference<>(new ArrayBlockingQueue<>(100));
    final AtomicInteger count = new AtomicInteger(0);
    final AtomicInteger capacityExceeded = new AtomicInteger(0);

    public QueueAppender(final String name) {
      super(name, null, null, true, Property.EMPTY_ARRAY);
      assert null != name;
      this.start();
    }

    @Override
    public void append(final LogEvent event) {
      final Queue<LogEvent> q = queue.get(); // read from reference once
      final LogEvent memento =
          (event instanceof MutableLogEvent) ? ((MutableLogEvent) event).createMemento() : event;
      final int currentCount = count.incrementAndGet();
      if (q.offer(memento)) {
        if (log.isDebugEnabled()) {
          log.debug(
              "{} recorded a log event (#{}; currentSize={})",
              this.getName(),
              currentCount,
              q.size());
        }
      } else {
        final int currentCapacityExceeded = capacityExceeded.incrementAndGet();
        if (log.isErrorEnabled()) {
          log.error(
              "{} processed a log event which exceeded capacity and could not be recorded (#{}; currentSize={}; numTimesCapacityExceeded={})",
              this.getName(),
              currentCount,
              q.size(),
              currentCapacityExceeded);
        }
      }
    }

    /**
     * The total number of LogEvents processed by this appender (not impacted by when/how changes
     * are made to the queue)
     */
    public int getCount() {
      return count.get();
    }

    /**
     * Returns the number of times this appender was unable to queue a LogEvent due to exceeding
     * capacity
     *
     * @see Queue#offer
     */
    public int getNumCapacityExceeded() {
      return capacityExceeded.get();
    }

    /** Changes the queue that will be used for any future events that are appended */
    public void setQueue(final Queue<LogEvent> q) {
      assert null != q;
      this.queue.set(q);
    }

    /** Returns Raw access to the (current) queue */
    public Queue<LogEvent> getQueue() {
      return queue.get();
    }
  }
}
