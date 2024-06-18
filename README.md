# Spark Job implemented as Spring cloud task

## References
* Running on Kubernetes [**`running-on-kubernetes`**](https://spark.apache.org/docs/3.4.1/running-on-kubernetes.html#cluster-mode).
* Running Spark Job from a Spring boot app [**`spring-boot-spark-on-minikube`**](https://www.itaydafna.dev/blog/spring-boot-spark-on-minikube/).
* Spark job submit, jar from local filesystem to minikube [**`spark-and-local-filesystem-in-minikube`**](https://jaceklaskowski.github.io/spark-kubernetes-book/demo/spark-and-local-filesystem-in-minikube/).

## Commonly used commands

```shell
kubectl delete pods --all
kubectl delete pods --all -n default

kubectl get pods
kubectl logs <pd name>
kubectl describe pod <pd name>

kubectl delete pod <pd name>
docker image build . -t spark-spring-cloud-task:0.0.1  -f Dockerfile --no-cache
docker images
docker rmi spark-spring-cloud-task:0.0.1

minikube image load spark-spring-cloud-task:0.0.1
minikube image rm spark-spring-cloud-task:0.0.1 

kubectl create serviceaccount spark -n telos
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=telos:spark --namespace=telos

kubectl delete clusterrolebinding spark-role

kubectl cluster-info
kubectl config view

./bin/spark-submit \
    --master k8s://https://127.0.0.1:58410 \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.kubernetes.container.image=spark-spring-cloud-task:0.0.1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.driverEnv.SPARK_USER=spark \
    --conf spark.executor.instances=2 \
    local:///opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar
    
./bin/spark-submit \
    --master k8s://https://127.0.0.1:58410 \
    --deploy-mode cluster \
    --name spark-spring \
    --class com.ksoot.spark.SparkTryMeTask \
    --conf spark.kubernetes.container.image=spark-spring-cloud-task:0.0.1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.driverEnv.SPARK_USER=spark \
    --conf spark.executor.instances=2 \
    local:///opt/spark/job-apps/spark-spring-cloud-task.jar
```

```shell
minikube mount /Users/rajveersingh/kubevol/spark-apps:/tmp/spark-apps

minikube ssh
ls -ld /tmp/spark-apps
ls /tmp/spark-apps
```

Providing the jar in the local filesystem mounted on minikube.
```shell
    
./bin/spark-submit \
    --master k8s://https://127.0.0.1:50926 \
    --deploy-mode cluster \
    --name spark-spring \
    --class com.ksoot.spark.SparkTryMeTask \
    --conf spark.kubernetes.container.image=officiallysingh/spark:3.4.1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.driverEnv.SPARK_USER=spark \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.file.upload.path=/tmp/spark-apps \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-host-mount.mount.path=/tmp/spark-apps \
    --conf spark.kubernetes.driver.volumes.hostPath.spark-host-mount.options.path=/tmp/spark-apps \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-host-mount.mount.path=/tmp/spark-apps \
    --conf spark.kubernetes.executor.volumes.hostPath.spark-host-mount.options.path=/tmp/spark-apps \
    local:///tmp/spark-apps/spark-spring-cloud-task.jar
```

> [!IMPORTANT]
Placeholder.

```java
Placeholder
```
