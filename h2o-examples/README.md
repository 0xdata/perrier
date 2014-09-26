# Sparkling Water Examples

## Available Examples
  * `ProstateDemo` - running K-means on prostate dataset (see
    _smalldata/prostate.csv_)
  * `DeepLearningDemo` - running DeepLearning on a subset of airlines dataset (see
    _smalldata/allyears2k\_headers.csv.gz_)

## Run example

### Simple local cluster
 
 Run a given example on local cluster. The cluster is defined by MASTER address
`local-cluster[3,2,3072]` which means that cluster contains 3 worker nodes, each having 2CPUs and 3GB of memory
   * Go to `h2o-examples`
   * Run `./run-example.sh <name of demo>`

### Run on Spark cluster
   * Run Spark cluster, for example via `sbin/launch-spark-cloud.sh`
     * Verify that Spark is running - Spark UI on `http://localhost:8080/` should show 3 worker nodes 
   * Export `MASTER` address of Spark master, i.e., `export MASTER="spark://localhost:7077"`
   * Go to `h2o-examples`
   * Run `./run-example.sh <name of demo>`
   * Observe status of the application via Spark UI on `http://localhost:8080/`

