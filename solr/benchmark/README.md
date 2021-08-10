### JMH-Benchmarks module

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.
Writing correct micro-benchmarks in Java (or another JVM language) is difficult and there are many non-obvious pitfalls (many
due to compiler optimizations). JMH is a framework for running and analyzing benchmarks (micro or macro) written in Java (or
another JVM language).

### Running benchmarks

If you want to set specific JMH flags or only run certain benchmarks, passing arguments via
gradle tasks is cumbersome. The process has been simplified by the provided `jmh.sh` script.

The default behavior is to run all benchmarks:

    ./jmh.sh

Pass a pattern or name after the command to select the benchmarks:

    ./jmh.sh CloudIndexing

Check which benchmarks match the provided pattern:

    ./jmh.sh -l CloudIndexing

Run a specific test and override the number of forks, iterations and warm-up iteration to `2`:

    ./jmh.sh -f 2 -i 2 -wi 2 CloudIndexing

Run a specific test with async and GC profilers on Linux and flame graph output:

    ./jmh.sh -prof gc -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph\;dir=profile-results CloudIndexing

### Using JMH with async profiler

It's good practice to check profiler output for micro-benchmarks in order to verify that they represent the expected
application behavior and measure what you expect to measure. Some example pitfalls include the use of expensive mocks
or accidental inclusion of test setup code in the benchmarked code. JMH includes
[async-profiler](https://github.com/jvm-profiling-tools/async-profiler) integration that makes this easy:

    ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;dir=profile-results

With flame graph output:

    ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph\;dir=profile-results

Simultaneous cpu, allocation and lock profiling with async profiler 2.0 and jfr output:

    ./jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=jfr\;alloc\;lock\;dir=profile-results CloudIndexing

A number of arguments can be passed to configure async profiler, run the following for a description:

    ./jmh.sh -prof async:help

You can also skip specifying libPath if you place the async profiler lib in a predefined location such as /usr/java/packages/lib

### Using JMH GC profiler

You can run a benchmark with `-prof gc` to measure its allocation rate:

    ./jmh.sh -prof gc:dir=profile-results

Of particular importance is the `norm` alloc rates, which measure the allocations per operation rather than allocations per second.

### Using JMH Java Flight Recorder profiler

JMH comes with a variety of built-in profilers. Here is an example of using JFR:

    ./jmh.sh -prof jfr:dir=profile-results\;configName=jfr-profile.jfc

In this example we point to the included configuration file with configName, but you could also do something like settings=default or settings=profile.

### Benchmark Repeatability

Indexes created for the benchmarks often involve randomness when generating terms, term length and number of terms in a field. In order to make benchmarks repeatable, a static seed is used for randoms. This allows for generating varying data while ensuring that data is consistent across runs.

You can vary that seed by setting a system property to explore a wider range of variation in the benchmark:

    -jvmArgsAppend -Dsolr.bench.seed=6624420638116043983

The seed used for a given benchmark run will be printed out near the top of the output.

    --> benchmark random seed: 6624420638116043983

You can also specify where to place the mini-cluster with a system property:

    -jvmArgsAppend -DminiClusterBaseDir=/benchmark-data/mini-cluster

In this case, new data will not be generated for the benchmark, even if you change parameters. The use case for this if you are running a query based benchmark and want to create a large index for testing and reuse (say hundreds of GB's). Be aware that with this system property set, that same mini-cluster will be reused for any benchmarks run, regardless of if that makes sense or not.

### Writing benchmarks

For help in writing correct JMH tests, the best place to start is the [sample code](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/) provided
by the JMH project.

JMH is highly configurable and users are encouraged to look through the samples for suggestions
on what options are available. A good tutorial for using JMH can be found [here](http://tutorials.jenkov.com/java-performance/jmh.html#return-value-from-benchmark-method)

Many Solr JMH benchmarks are actually closer to a full integration benchmark in that they run a single action against a full Solr mini cluster.

See org.apache.solr.bench.index.CloudIndexing for an example of this.

#### MiniCluster Metrics

After every iteration, the metrics collected by Solr will be dumped to the build/work/metrics-results folder. You can disable metrics collection using the metricsEnabled method of the MiniClusterState, in which case the same output files will be dumped, but the values will all be 0/null.

### JMH Options
Some common JMH options are:

```text

   -e <regexp+>                Benchmarks to exclude from the run. 

   -f <int>                    How many times to fork a single benchmark. Use 0 to 
                               disable forking altogether. Warning: disabling 
                               forking may have detrimental impact on benchmark 
                               and infrastructure reliability, you might want 
                               to use different warmup mode instead.

   -i <int>                    Number of measurement iterations to do. Measurement
                               iterations are counted towards the benchmark score.
                               (default: 1 for SingleShotTime, and 5 for all other
                               modes)

   -l                          List the benchmarks that match a filter, and exit.

   -lprof                      List profilers, and exit.

   -o <filename>               Redirect human-readable output to a given file. 

   -prof <profiler>            Use profilers to collect additional benchmark data. 
                               Some profilers are not available on all JVMs and/or 
                               all OSes. Please see the list of available profilers 
                               with -lprof.

   -v <mode>                   Verbosity mode. Available modes are: [SILENT, NORMAL,
                               EXTRA]

   -wi <int>                   Number of warmup iterations to do. Warmup iterations
                               are not counted towards the benchmark score. (default:
                               0 for SingleShotTime, and 5 for all other modes)
                               
  -t <int>                    Number of worker threads to run with. 'max' means
                              the maximum number of hardware threads available
                              on the machine, figured out by JMH itself.
                                
  -rf <type>                  Format type for machine-readable results. These
                              results are written to a separate file (see -rff).
                              See the list of available result formats with -lrf.

  -rff <filename>             Write machine-readable results to a given file.
                              The file format is controlled by -rf option. Please
                              see the list of result formats for available formats.
                              
  -bm <mode>                  Benchmark mode. Available modes are: [Throughput/thrpt,
                              AverageTime/avgt, SampleTime/sample, SingleShotTime/ss,
                              All/all]                              
```

To view all options run jmh with the -h flag. 