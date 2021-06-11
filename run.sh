
# ./gradlew clean
# ./gradlew -p solr dev

bin_solr=./solr/packaging/build/solr-9.0.0-SNAPSHOT/bin/solr

$bin_solr start -e cloud -noprompt

sleep 30

for poc in 3 2 1 0
do
  curl --silent "http://localhost:8983/solr/gettingstarted/select?q=*:*&shards.info=true&poc=$poc"
done

$bin_solr stop -all

