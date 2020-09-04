# CKA

考前设置

vim .vimrc 

```
set paste
```

source .vimrc

```
alias kl=kubectl
```

## IMPORTANT INSTRUCTIONS:

Each question on this exam must be completed on a designated cluster/configuration context. To minisize switching. the questions are grouped so that all questions on a given cluster appear con

There are six clusters that comprise the exam environment. made up of varying number of containers. as follows:

- k8s-1 etcd, 1 master, 2 worker
- hk8s- 1 etcd, 1 master, 2 worker
- bk8s- 1 etcd, 1 master, 1 worker
- wk8s- 1 etcd, 1 master, 2 worker
- ek8s- 1 etcd, 1 master, 2 worker
- dk8s- 1 etcd ,1 master, 2 worker

## No.1

Set configuration context `$ kubectl config use-context k8s`

Monitor the logs of Pod foo and:

- Extract log lines corresponding to error `unable-to-access-website`
- Write them to `/opt/KULM00201/foo`

Question weight: 5%

Question: 2/25

Solution：

```

```

## No.2

Set configuration context `$ kubectl config use-context k8s`

List all PVs sorted by name, saving the full `kubectl` output to `/opt/KUCC00102/persistent_volumes`. Use `kubectl` own functionality for sorting the output, and do not manipulate it any further.

Question weight: 3%

Question: 3/25

Solution：

```

```

## No.3

Set configuration context `$ kubectl config use-context k8s`

Ensure a single instance of Pod `nginx` is running on each node of the Kubernetes cluster where `nginx` also represents the image name which has to be used. Do not override any taints currently in place.

Use DaemonSets to complete this task and use `ds-kusc00201` as DaemonSet name.

Question weight: 3%

Question: 4/25

Solution：

```

```



## No.4

Set configuration context `$ kubectl config use-context k8s`

Perform the following tasks:

- Add an init container to `hungry-bear`(which has been defined in spec file `/opt/KUCC00108/pod-spec-KUCC00108.yaml`)
- The init container should:create an empty file named `/workdir/calm.txt`
- if `/workdir/calm.txt` is not detected, the Pod should exit
- Once the spec file has been updated with the init container definition, the Pod should be created

```
/opt/KUCC00108/pod-spec-KUCC00108.yaml:
apiVersion: v1
kind: Pod
metadata:
  name: hungry-bear
spec:
  volumes:
    - name: workdir
      emptyDir: {}
  containers:
  - name: checker
    image: alpine
    command: ["/bin/sh", "-c", "if [ -f /workdir/faithful.txt]; then sleep 100000; else exit 1; fi"]
    volumeMounts:
    - name: workdir
      mountPath: /workdir
```

Question weight: 3%

Question:4/25

Solution：

```

```

## No.5

Set configuration context `$ kubectl config use-context k8s`

Create a pod named `kucc4`  with a single container for each of the following images running inside(there may be between 1 and 4 images specified): `nginx + redis`

Question weight: 4%

Question: 6/25

Solution：

```

```

## No.6

Set configuration context `$ kubectl config use-context k8s`

Create a pod as follows：

- Name：`nginx-kusc00101`
- Image：`nginx`
- Node selector：`disk=spinning`

前置条件：

```
kubectl label node minikube disk=spinning
```

Question weight: 2%

Question: 7/25

Solution：

```

```

## No.7

Set configuration context `$ kubectl config use-context k8s`

create a deployment  as follows：

- Name: `nginx-app`
- Using container nginx with version1.11.10-alpine
- The deployment should contain 3 replicas

Next, deploy the application with new version 1.12.0-alpine, by performing a rolling update.

Finally, rollback that update to the previous version 1.11.10-alpine

Question weight: 4%

Question: 8/25

Solution：

```

```

## No.8

Set configuration context `$ kubectl config use-context k8s`

Create and configure the service `front-end-service` so it's accessible through `NodePort` and routes to the existing pod named `front-end`

Question weight: 4%

Question: 9/25

Solution：

```

```

## No.9

Set configuration context `$ kubectl config use-context k8s`

Create a Pod as follows:

- Name: `nginx`
- Using image: `nginx`
- In a new Kubernetes namespace named: `website-backend`

Question weight: 3%

Question: 10/25

Solution：

```

```

## No.10

Set configuration context `$ kubectl config use-context k8s`

Create a deployment spec file that will:

- Launch: `3` replicas of the` redis` image with the label:`pipeline_stage=test`
- Deployment name: `kual00201`

Save a copy of this spec file to `/opt/KUAL00201/spec_deploy.yaml`(or .json).

When you are done, clean up(delete) any new k8s API objects that you produced during this disk.

Question weight: 3%

Question: 11/25

Solution：

```

```

## No.11

Set configuration context `$ kubectl config use-context k8s`

Create a file: `/opt/KUCC00302/kucc00203.txt` that lists all pods that implement Service bar in Namespace default.

The format of the file should be one pod name per line.

Question weight: 3%

Question: 12/25

前置条件：

```
kubectl run bar --image=nginx
kubectl run bar --image=nginx--restart=Never
kubectl expose pod bar --port=80
```

Solution：

```

```

## No.12

Set configuration context `$ kubectl config use-context k8s`

Create a Kubernetes Secret as follows:

- Name: `super-secret`
- username: `alice`

Create a Pod named `pod-secrets-via-file`, using the` redis` image, which mounts a secret named super-secret as `/secrets `

Create a second Pod named `pod-secrets-via-env`, using the `redis` image, which exports username as `CONFIDENTIAL`

Question weight: 9%

Question: 13/25

Solution：

```

```

## No.13

Set configuration context `$ kubectl config use-context k8s`

Create a pod as follows:

- Name: `non-persistent-redis`

- Container image: `redis`
- Named-volume with name: `app-cache`
- Mount path: `/data/redis`

It should launch in the `qa` namespace and the volume MUST NOT be persistent.

Question weight: 4%

Question: 14/25

Solution：

```

```

## No14

Set configuration context `$ kubectl config use-context k8s`

Scale the deployment guestbook to 3 pods

Question weight: 1%

Question: 15/25

前置条件：

```
kubectl create deployment guestbook --image=nginx:1.14
```

Solution：

```

```

## No.15

Set configuration context `$ kubectl config use-context k8s`

Check to see how many nodes are ready(not including nodes tainted NoSchedule) and write the number to /opt/KUCC00104/kucc00104.txt

Question weight: 2%

Question: 16/25

Solution：

```

```

## No.16

Set configuration context `$ kubectl config use-context k8s`

From the Pod label name=cpu-loader, find pods running high CPU workloads and write the name of the Pod consuming most CPU to the file `/opt/KUTR00102/KUTR00102.txt`(which already exists)

Question weight: 2%

Question: 17/25

Solution：

```

```

## No.17

Set configuration context `$ kubectl config use-context k8s`

Create a deployment as follows:

- Name: `nginx-dns`
- Exposed via a service: `nginx-dns`
- Ensure that the service & pod are accessible via their respective DNS records
- The container(s) within any Pod(s) running as a part of this deployment should use the nginx image.

Next, use the utility `nslookup` to look up the DNS records of the service & pod and write the output to `/opt/KUNW00601/service.dns` and `/opt/KUNW00601/pod.dns` respectively.

Question weight: 7%

Question: 18/25

Solution：

```

```



## No.18

Set configuration context `$ kubectl config use-context k8s`

Create a snapshot of the `etcd` instance running at `htpps://127.0.0.1:2379`, saving the snapshot to the file path `/var/lib/backup/etc-snapshot.db`

The `etcd` instance is running `etcd` version 3.1.10

The following TLS certificates/key are supplied for connecting to the server with `etcdctl`:

- CA certificate: `/opt/KUCM00302/ca.crt`
- Client certificate: `/opt/KUCM00302/etcd-client.crt`
- Client key: `/opt/KUCM00302/etcd-client.key`

Question weight: 7%

Question: 19/25

Solution：

```

```



## No.19

Set configuration context `$ kubectl config use-context k8s`

Set the node labelled with `name=ek8s-node-0` as unavailable and reschedule all the pods running on it

Question weight: 4%

Question: 20/25

Solution：

```

```



## No.20

Set configuration context `$ kubectl config use-context k8s`

A Kubernetes worker node, labelled with `name=wk8s-node-0` in tate `NotReady`. Investigate why this is the case, and perform any appropriate steps to bring the node to a `Ready` state, ensuring that any changes are made permanent.

Hints:

- You can `ssh` to the failed node using : `$ssh wk8s-node-0`
- You can assume elevated privileges on the node with the following command: `$ sudo -i`

Question weight: 4%

Question: 21/25

Solution：

```

```



## No.21

Set configuration context `$ kubectl config use-context k8s`

Configure the `kubelet systemd` managed service, on the node labelled with name=wk8s-node-1, to launch a Pod containing a single container of image `jenkins` named `myservice` automatically. Any spec files required should be placed in the `/etc/kubernetes/manifests` directory on the node.

Hints:

- You can ssh to the failed node using : `$ssh wk8s-node-1`
- You can assume elevated privileges on the node with the following command: `$ sudo -i`

Question weight: 4%

Question: 22/25

```

```

## No.22

No configuration context change required for this item.

For this item, you will have to ssh to the nodes ik8s-master-0 and ik8s-node-0 and complete all tasks on these nodes. Ensure that you return to the base node(hostname:node-1) when you have completed thi item.

**Context**

As an administrator of a small development team, you have been asked to set up a Kubernetes cluster to test the viability of new application.

**Task**

You must use `kubeadm` to perform this task.Any `kubeadm` invocations will require the use of the `--ignore-preflight-errors=all` option.

- Configure the node ik8s-master-0 as a master node
- Join the node ik8s-node-0 to the cluster

You must use the `kubeadm` configuration file located at `/etc/kubeadm.conf `when initializing you cluster.

The cluster will be considered complete once both nodes are in **Ready** state.

Docker is already installed on both nodes and apt has been configured so that you can install the required tools.

```

```

## No.23

Set configuration context `$ kubectl config use-context k8s`

Given a partially-functioning Kubernetes cluster, identify symptoms of failure on the cluster.

Determine the node, the failing service and take actions to bringup the failed service and restore the health of the cluster. Ensure that any changes are made permanently.

The worker node in this cluster is labelled with `name=bk8s-node-0`

Hints:

- You can ssh to the relevant nodes using: $  ssh  ​${NODE} where ${NODE} is one of bk8s-master-0 or bk8s-node-0
- You can assume elevated privileges on any node in the cluster with the following command: `$ sudo -i`

Question weight: 4%

Question: 24/25

Solution：

```
kubectl get cs
```

## No.24

Set configuration context `$ kubectl config use-context k8s`

Create persistent volume with name `app-config`, of capacity 1Gi and access mode `ReadWriteMany`. The type of volume is `hostPath` and its location is `/srv/app-config`

Question weight: 4%

Question: 22/25

Solution：

```

```

