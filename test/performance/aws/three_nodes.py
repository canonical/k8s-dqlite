# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import urllib.request
import uuid
import paramiko
import os

import boto3
from alive_progress import alive_bar
from rich.console import Console

from instance import EC2InstanceWrapper
from key_pair import KeyPairWrapper
from security_group import SecurityGroupWrapper

logger = logging.getLogger(__name__)
console = Console()

# snippet-start:[python.example_code.ec2.Scenario_GetStartedInstances]
class EC2InstanceScenario:
    """
    A scenario that demonstrates how to use Boto3 to manage Amazon EC2 resources.
    Covers creating a key pair, security group, launching an instance, and cleaning up resources.
    """

    def __init__(
        self,
        inst_wrapper: EC2InstanceWrapper,
        key_wrapper: KeyPairWrapper,
        sg_wrapper: SecurityGroupWrapper,
        ssm_client: boto3.client,
        remote_exec: bool = False,
    ):
        """
        Initializes the EC2InstanceScenario with the necessary AWS service wrappers.

        :param inst_wrapper: Wrapper for EC2 instance operations.
        :param key_wrapper: Wrapper for key pair operations.
        :param sg_wrapper: Wrapper for security group operations.
        :param ssm_client: Boto3 client for accessing SSM to retrieve AMIs.
        :param remote_exec: Flag to indicate if the scenario is running in a remote execution
                            environment. Defaults to False. If True, the script won't prompt
                            for user interaction.
        """
        self.inst_wrapper = inst_wrapper
        self.key_wrapper = key_wrapper
        self.sg_wrapper = sg_wrapper
        self.ssm_client = ssm_client
        self.remote_exec = remote_exec

    def create_and_list_key_pairs(self) -> None:
        """
        Creates an RSA key pair for SSH access to the EC2 instance and lists available key pairs.
        """
        console.print("**Step 1: Create a Secure Key Pair**", style="bold cyan")
        console.print(
            "Let's create a secure RSA key pair for connecting to your EC2 instance."
        )
        key_name = f"MyUniqueKeyPair-{uuid.uuid4().hex[:8]}"
        console.print(f"- **Key Pair Name**: {key_name}")

        # Create the key pair and simulate the process with a progress bar.
        with alive_bar(1, title="Creating Key Pair") as bar:
            self.key_wrapper.create(key_name)
            time.sleep(0.4)  # Simulate the delay in key creation
            bar()

        console.print(f"- **Private Key Saved to**: {self.key_wrapper.key_file_path}\n")
        os.chmod(self.key_wrapper.key_file_path, 0o400)

        # List key pairs (simulated) and show a progress bar.
        list_keys = True
        if list_keys:
            console.print("- Listing your key pairs...")
            start_time = time.time()
            with alive_bar(100, title="Listing Key Pairs") as bar:
                while time.time() - start_time < 2:
                    time.sleep(0.2)
                    bar(10)
                self.key_wrapper.list(5)
                if time.time() - start_time > 2:
                    console.print(
                        "Taking longer than expected! Please wait...",
                        style="bold yellow",
                    )

    def create_security_group(self) -> None:
        """
        Creates a security group that controls access to the EC2 instance and adds a rule
        to allow SSH access from the user's current public IP address.
        """
        console.print("**Step 2: Create a Security Group**", style="bold cyan")
        console.print(
            "Security groups manage access to your instance. Let's create one."
        )
        sg_name = f"MySecurityGroup-{uuid.uuid4().hex[:8]}"
        console.print(f"- **Security Group Name**: {sg_name}")

        # Create the security group and simulate the process with a progress bar.
        with alive_bar(1, title="Creating Security Group") as bar:
            self.sg_wrapper.create(
                sg_name, "Security group for example: get started with instances."
            )
            time.sleep(0.5)
            bar()

        console.print(f"- **Security Group ID**: {self.sg_wrapper.security_group}\n")

        # Get the current public IP to set up SSH access.
        ip_response = urllib.request.urlopen("http://checkip.amazonaws.com")
        current_ip_address = ip_response.read().decode("utf-8").strip()
        console.print(
            "Let's add a rule to allow SSH only from your current IP address."
        )
        console.print(f"- **Your Public IP Address**: {current_ip_address}")
        console.print("- Automatically adding SSH rule...")

        # Update security group rules to allow SSH and simulate with a progress bar.
        with alive_bar(1, title="Updating Security Group Rules") as bar:
            response = self.sg_wrapper.authorize_ingress(current_ip_address)
            time.sleep(0.4)
            if response and response.get("Return"):
                console.print("- **Security Group Rules Updated**.")
            else:
                console.print(
                    "- **Error**: Couldn't update security group rules.",
                    style="bold red",
                )
            bar()

        self.sg_wrapper.describe(self.sg_wrapper.security_group)
        
    def create_instances(self) -> None:
        """
        Creates 3 etcd instances using a specified Ubuntu AMI and instance type.
        Displays instance details and SSH connection information.
        """
        ami_img = "ami-084568db4383264d4" # Ubuntu 24.04 LTS x86
        instance_type = "c6a.large"  # Instance type
        key = self.key_wrapper.key_pair["KeyName"]
        security_group = self.sg_wrapper.security_group
        
        for _ in range(3):
            self.inst_wrapper.create(
                ami_img,
                instance_type,
                key,
                [security_group],
            )
        self.inst_wrapper.display()
        self._display_ssh_info()
           
    def create_instance(self) -> None:
        """
        Launches an EC2 instance using an Amazon Linux 2 AMI and the created key pair
        and security group. Displays instance details and SSH connection information.
        """
        # Retrieve Amazon Linux 2 AMIs from SSM.
        ami_paginator = self.ssm_client.get_paginator("get_parameters_by_path")
        ami_options = []
        for page in ami_paginator.paginate(Path="/aws/service/ami-amazon-linux-latest"):
            ami_options += page["Parameters"]
        amzn2_images = self.inst_wrapper.get_images(
            [opt["Value"] for opt in ami_options if "amzn2" in opt["Name"]]
        )
        console.print("\n**Step 3: Launch Your Instance**", style="bold cyan")
        console.print(
            "Let's create an instance from an Amazon Linux 2 AMI. Here are some options:"
        )
        image_choice = 0
        console.print(f"- Selected AMI: {amzn2_images[image_choice]['ImageId']}\n")

        # Display instance types compatible with the selected AMI
        inst_types = self.inst_wrapper.get_instance_types(
            amzn2_images[image_choice]["Architecture"]
        )
        inst_type_choice = 0
        console.print(
            f"- Selected instance type: {inst_types[inst_type_choice]['InstanceType']}\n"
        )

        console.print("Creating your instance and waiting for it to start...")
        with alive_bar(1, title="Creating Instance") as bar:
            self.inst_wrapper.create(
                amzn2_images[image_choice]["ImageId"],
                inst_types[inst_type_choice]["InstanceType"],
                self.key_wrapper.key_pair["KeyName"],
                [self.sg_wrapper.security_group],
            )
            time.sleep(21)
            bar()

        console.print(f"**Success! Your instance is ready:**\n", style="bold green")
        self.inst_wrapper.display()

        console.print(
            "You can use SSH to connect to your instance. "
            "If the connection attempt times out, you might have to manually update "
            "the SSH ingress rule for your IP address in the AWS Management Console."
        )
        self._display_ssh_info()
    
    def execute_command_instance(self, commands=[], index=0, push_files=[], pull_files=[]) -> None:
        """
        Executes a command on the EC2 instance using the provided command string.
        This method is intended for remote execution scenarios.
        
        :param command: The command to execute on the EC2 instance.
        """
        if self.inst_wrapper.instances:
                instance = self.inst_wrapper.instances[index]
                instance_id = instance["InstanceId"]

                waiter = self.inst_wrapper.ec2_client.get_waiter("instance_running")
                console.print(
                    "Waiting for the instance to be in a running state with a public IP...",
                    style="bold cyan",
                )

                with alive_bar(1, title="Waiting for Instance to Start") as bar:
                    waiter.wait(InstanceIds=[instance_id])
                    time.sleep(20)
                    bar()

                instance = self.inst_wrapper.ec2_client.describe_instances(
                    InstanceIds=[instance_id]
                )["Reservations"][0]["Instances"][0]

                public_ip = instance.get("PublicIpAddress")
                private_key_path = self.key_wrapper.key_file_path
                os.chmod(self.key_wrapper.key_file_path, 0o400)
                print(f"Permissions for '{self.key_wrapper.key_file_path}' set to 400.")
                
                private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                print(f"Connecting to ubuntu@{public_ip} using key {private_key_path}...")
                client.connect(
                    hostname=public_ip,
                    username="ubuntu",
                    pkey=private_key,
                    timeout=10 # seconds to wait for connection
                )
                print("Connection successful!")
                for command in commands:
                    print(f"\nExecuting command: {command}")
                    _, stdout, stderr = client.exec_command(command)
                    output = stdout.read().decode().strip()
                    error = stderr.read().decode().strip()
                    exit_status = stdout.channel.recv_exit_status() # Get the command's exit code

                    print(f"\n--- Command Output ---")
                    if output:
                        print(f"STDOUT:\n{output}")
                    if error:
                        print(f"STDERR:\n{error}")
                    print(f"Exit Status: {exit_status}")
                
                
                for file in push_files:
                    sftp_client = client.open_sftp()
                    sftp_client.put(file[0], file[1])
                
                for file in pull_files:
                    sftp_client = client.open_sftp()
                    sftp_client.get(file[0], file[1])
                client.close()

    def _display_ssh_info(self) -> None:
        """
        Displays SSH connection information for the user to connect to the EC2 instance.
        Handles the case where the instance does or does not have an associated public IP address.
        """
        if self.inst_wrapper.instances:
            for instance in self.inst_wrapper.instances:
                instance_id = instance["InstanceId"]

                waiter = self.inst_wrapper.ec2_client.get_waiter("instance_running")
                console.print(
                    "Waiting for the instance to be in a running state with a public IP...",
                    style="bold cyan",
                )

                with alive_bar(1, title="Waiting for Instance to Start") as bar:
                    waiter.wait(InstanceIds=[instance_id])
                    time.sleep(20)
                    bar()

                instance = self.inst_wrapper.ec2_client.describe_instances(
                    InstanceIds=[instance_id]
                )["Reservations"][0]["Instances"][0]

                public_ip = instance.get("PublicIpAddress")
                if public_ip:
                    console.print(
                        "\nTo connect via SSH, open another command prompt and run the following command:",
                        style="bold cyan",
                    )
                    console.print(
                        f"\tssh -i {self.key_wrapper.key_file_path} ubuntu@{public_ip}"
                    )
                else:
                    console.print(
                        "Instance does not have a public IP address assigned.",
                        style="bold red",
                    )
        else:
            console.print(
                "No instance available to retrieve public IP address.",
                style="bold red",
            )

        if not self.remote_exec:
            console.print("\nOpen a new terminal tab to try the above SSH command.")
            input("Press Enter to continue...")

    def stop_and_start_instance(self) -> None:
        """
        Stops and restarts the EC2 instance. Displays instance state.
        """
        console.print("\n**Step 5: Stop and Start Your Instance**", style="bold cyan")
        console.print("Let's stop and start your instance to see what changes.")
        console.print("- **Stopping your instance and waiting until it's stopped...**")

        with alive_bar(1, title="Stopping Instance") as bar:
            self.inst_wrapper.stop()
            time.sleep(360)
            bar()

        console.print("- **Your instance is stopped. Restarting...**")

        with alive_bar(1, title="Starting Instance") as bar:
            self.inst_wrapper.start()
            time.sleep(20)
            bar()

        console.print("**Your instance is running.**", style="bold green")
        self.inst_wrapper.display()

        self._display_ssh_info()

    def cleanup(self) -> None:
        """
        Cleans up all the resources created during the scenario, including terminating the instance, deleting the security
        group, and deleting the key pair.
        """
        console.print("\n**Step 6: Clean Up Resources**", style="bold cyan")
        console.print("Cleaning up resources:")
        console.print(f"- **Instance**: {self.inst_wrapper.instances[0]['InstanceId']}")

        with alive_bar(1, title="Terminating Instance") as bar:
            self.inst_wrapper.terminate()
            time.sleep(380)
            bar()

        console.print("\t- **Terminated Instance**")

        console.print(f"- **Security Group**: {self.sg_wrapper.security_group}")

        with alive_bar(1, title="Deleting Security Group") as bar:
            self.sg_wrapper.delete(self.sg_wrapper.security_group)
            time.sleep(1)
            bar()

        console.print("\t- **Deleted Security Group**")

        console.print(f"- **Key Pair**: {self.key_wrapper.key_pair['KeyName']}")

        with alive_bar(1, title="Deleting Key Pair") as bar:
            self.key_wrapper.delete(self.key_wrapper.key_pair["KeyName"])
            time.sleep(0.4)
            bar()

        console.print("\t- **Deleted Key Pair**")

    def run_scenario(self) -> None:
        """
        Executes the entire EC2 instance scenario: creates key pairs, security groups,
        launches an instance, and cleans up all resources.
        """
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

        console.print("-" * 88)
        console.print(
            "Welcome to the AWS Performance test 3 node scenario!",
            style="bold magenta",
        )
        console.print("-" * 88)

        self.create_and_list_key_pairs()
        self.create_security_group()
        self.create_instances()
        microk8s_commands = [
            "sudo apt update",
            "sudo snap install microk8s --channel=1.31 --classic",
            "sudo microk8s status --wait-ready",
            "mkdir -p ~/.kube/",
            "sudo microk8s config > ~/.kube/config",
            "sudo apt-get install -y sysstat"
        ]
        etcd_commands = [
            "sudo apt update",
            "sudo snap install microk8s --channel=1.31 --classic",
            "sudo microk8s status --wait-ready",
            "mkdir -p ~/.kube/",
            "sudo microk8s config > ~/.kube/config",
            "sudo apt-get install -y sysstat"
            "sudo microk8s stop",
            "sudo mv /var/snap/microk8s/current/var/lock/no-etcd /var/snap/microk8s/current/var/lock/yes-etcd",
            "sudo touch /var/snap/microk8s/current/var/lock/no-k8s-dqlite",
            "sudo sed -i '/--storage-backend/d' /var/snap/microk8s/current/args/kube-apiserver",
            "sudo sed -i '/--storage-dir/d' /var/snap/microk8s/current/args/kube-apiserver",
            "sudo sed -i '/--etcd-servers/d' /var/snap/microk8s/current/args/kube-apiserver",
            "echo --etcd-servers=https://172.31.34.85:12379,https://172.31.37.184:12379,https://172.31.32.86:12379 | sudo tee -a /var/snap/microk8s/current/args/kube-apiserver",
            "echo --etcd-cafile=/var/snap/microk8s/current/certs/ca.crt | sudo tee -a /var/snap/microk8s/current/args/kube-apiserver",
            "echo --etcd-certfile=/var/snap/microk8s/current/certs/server.crt | sudo tee -a /var/snap/microk8s/current/args/kube-apiserver",
            "echo --etcd-keyfile=/var/snap/microk8s/current/certs/server.key | sudo tee -a /var/snap/microk8s/current/args/kube-apiserver",
            "sudo microk8s start",
            "sudo microk8s status --wait-ready",
            "sudo microk8s kubectl apply -f /var/snap/microk8s/current/args/cni-network/cni.yaml"
        ]

        prom_commands = [
            "sudo microk8s enable prometheus",
            "sleep 40",
            "nohup sudo microk8s.kubectl port-forward -n observability svc/kube-prom-stack-grafana 30801:80 --address='0.0.0.0' > /dev/null 2>&1 &"
            
        ]
        kube_burner_commands = [
            "git clone https://github.com/canonical/k8s-dqlite.git",
            "mkdir -p ../bin/",
            "wget https://github.com/kube-burner/kube-burner/releases/download/v1.2/kube-burner-1.2-Linux-x86_64.tar.gz",
            "tar -zxvf kube-burner-1.2-Linux-x86_64.tar.gz",
            "chmod +x /root/kube-burner"
        ]
        
        self.execute_command_instance(etcd_commands, 0)
        self.execute_command_instance(etcd_commands, 1)
        self.execute_command_instance(etcd_commands, 2)
        self.execute_command_instance(kube_burner_commands, 0)
        self.execute_command_instance(prom_commands, 0)
        self._display_ssh_info()
        ## Join cluster, do dashboard in grafana, collect metrics, run kube-burner
        
        # self.stop_and_start_instance()
        # self.cleanup()

        console.print("\nThanks for watching!", style="bold green")
        console.print("-" * 88)


if __name__ == "__main__":
    try:
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        scenario = EC2InstanceScenario(
            EC2InstanceWrapper.from_client(),
            KeyPairWrapper(boto3.client("ec2"), "/Users/louiseschmidtgen"),
            SecurityGroupWrapper.from_client(),
            boto3.client("ssm"),
        )
        scenario.run_scenario()
    except Exception:
        logging.exception("Something went wrong with the demo.")
# snippet-end:[python.example_code.ec2.Scenario_GetStartedInstances]