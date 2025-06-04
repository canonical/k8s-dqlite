sudo apt update
sudo snap install microk8s --channel=1.31 --classic
sudo microk8s status --wait-ready
sudo microk8s enable prometheus
mkdir -p ~/.kube/
sudo microk8s config >~/.kube/config
sudo apt-get install -y sysstat

## Kube-burner
mkdir -p ../bin/
wget "https://github.com/kube-burner/kube-burner/releases/download/v1.2/kube-burner-1.2-Linux-x86_64.tar.gz"
tar -zxvf kube-burner-1.2-Linux-x86_64.tar.gz
sudo chmod +x /home/ubuntu/kube-burner

# grafana
sudo microk8s.kubectl port-forward -n observability svc/kube-prom-stack-grafana 30801:80 --address='0.0.0.0'
# (user/pass: admin/prom-operator)

git clone https://github.com/canonical/k8s-dqlite.git
cd /home/ubuntu/k8s-dqlite/test/performance/templates
/home/ubuntu/kube-burner init --timeout 10m -c api-intensive.yaml

# make cluster
sudo microk8s add-node
sudo microk8s join xxx

#
pidstat -druh -p $(pgrep k8s-dqlite) 1 >/home/ubuntu/node1_metrics.log
pidstat -druh -p $(pgrep k8s-dqlite) 1 >/home/ubuntu/node2_metrics.log
pidstat -druh -p $(pgrep k8s-dqlite) 1 >/home/ubuntu/node3_metrics.log
