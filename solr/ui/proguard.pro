-keep class org.apache.solr.ui.domain.** { *; }
-keep class org.apache.solr.ui.*.data.** { *; }
-keep class org.apache.solr.ui.*.domain.** { *; }

-keep class androidx.compose.runtime.** { *; }
-keep class androidx.collection.** { *; }
-keep class androidx.lifecycle.** { *; }

# Kotlinx coroutines rules seems to be outdated with the latest version of Kotlin and Proguard
-keep class kotlinx.coroutines.** { *; }

# Don't warn about logback, as it is needed only during compile-time
-dontwarn ch.qos.logback.**
-dontwarn com.oracle.svm.core.annotate.**
-dontwarn io.github.oshai.kotlinlogging.logback.internal.**
# We're excluding Material 2 from the project as we're using Material 3
-dontwarn androidx.compose.material.**
