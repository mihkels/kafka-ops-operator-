# Kafka Operations Operator (kafka-ops-operator)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

Kafka Operations Operator is a Kubernetes operator that enables declarative management of Kafka operations using custom resources. It allows platform engineers and developers to perform operational tasks on Kafka topics without direct access to Kafka administration tools.

Currently, the operator supports the following operations:
- **ResetTopic**: Temporarily reduce topic retention settings to clear messages, then restore original or specified retention settings

## Architecture

The operator follows the Kubernetes operator pattern:

1. **Custom Resource Definition (CRD)**: Defines `KafkaOperation` resources that specify the desired Kafka operations
2. **Controller**: Watches for `KafkaOperation` resources and reconciles the desired state
3. **Kafka Integration**: Connects to Kafka clusters using the Sarama client library to execute operations

The operator implements a state machine for tracking operation progress:
- **Pending**: Initial state when operation is created
- **Confirming**: Waiting for confirmation (if autoConfirm is false)
- **InProgress**: Executing the operation
- **WaitingForRestore**: For ResetTopic, waiting before restoring original settings
- **Completed**: Operation successfully completed
- **Failed**: Operation failed
- **Cancelled**: Operation was cancelled

## Prerequisites

- [Go](https://golang.org/doc/install) (version matching go.mod)
- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- Access to a Kubernetes cluster (e.g., [kind](https://kind.sigs.k8s.io/) or Minikube)
- [Operator SDK](https://sdk.operatorframework.io/docs/installation/) (optional, for development)
- Access to a Kafka cluster

## Installation

### Build from Source

1. **Build the Operator Binary**:
   ```sh
   make build
   ```

2. **Build the Docker Image**:
   ```sh
   make docker-build IMG=<your-repo>/kafka-ops-operator:latest
   ```

3. **Push the Docker Image**:
   ```sh
   make docker-push IMG=<your-repo>/kafka-ops-operator:latest
   ```

### Deploy to Kubernetes

1. **Install CRDs**:
   ```sh
   make install
   ```

2. **Deploy the Operator**:
   ```sh
   make deploy IMG=<your-repo>/kafka-ops-operator:latest
   ```

### Using Helm Chart

A Helm chart is available for easy deployment:

```sh
cd helm-charts/kafka-ops-operator
helm install kafka-ops-operator .
```

## Usage

### ResetTopic Operation

The ResetTopic operation allows you to clear messages from a Kafka topic by temporarily setting a very low retention value, then restoring the original or specified retention settings.

1. **Create a KafkaOperation resource**:

```yaml
apiVersion: operations.kafkaops.io/v1alpha1
kind: KafkaOperation
metadata:
  name: reset-my-topic
spec:
  operation: ResetTopic
  clusterName: my-kafka-cluster      # Name of the Kafka cluster
  clusterNamespace: kafka-system     # Namespace where the Kafka cluster is deployed
  topicName: my-topic                # Name of the topic to reset
  retentionBytes: 1                  # Temporary retention bytes (set very low to clear messages)
  retentionMs: 1000                  # Optional: Temporary retention time in milliseconds
  restoreRetentionBytes: 1073741824  # Optional: Retention bytes to restore after reset (1GB)
  restoreRetentionMs: 86400000       # Optional: Retention time to restore after reset (24h)
  autoConfirm: true                  # Whether to execute automatically without confirmation
  timeoutSeconds: 300                # Timeout for the operation
```

Here's a real-world example from the included sample:

```yaml
apiVersion: operations.kafkaops.io/v1alpha1
kind: KafkaOperation
metadata:
  name: reset-motivation-topic
spec:
  operation: ResetTopic
  clusterName: digital-ocean-cluster
  clusterNamespace: kafka-worker
  topicName: motivation
  timeoutSeconds: 30
  retentionBytes: 1
  autoConfirm: true
```

2. **Apply the resource**:
   ```sh
   kubectl apply -f reset-topic.yaml
   ```

3. **Check the operation status**:
   ```sh
   kubectl get kafkaoperation reset-my-topic -o yaml
   ```

### Operation States

The `status.state` field in the KafkaOperation resource indicates the current state of the operation:

- **Pending**: The operation has been created but not yet started
- **Confirming**: Waiting for manual confirmation (if autoConfirm is false)
- **InProgress**: The operation is currently being executed
- **WaitingForRestore**: For ResetTopic, the retention has been reduced and waiting to restore
- **Completed**: The operation has completed successfully
- **Failed**: The operation has failed (check status.message for details)
- **Cancelled**: The operation was cancelled

## Custom Resource Definition (CRD)

### KafkaOperation Spec Fields

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| operation | string | Operation type (currently only "ResetTopic") | Yes |
| topicName | string | Name of the Kafka topic | Yes |
| clusterName | string | Name of the Kafka cluster | Yes |
| clusterNamespace | string | Namespace of the Kafka cluster | No (defaults to the same namespace) |
| retentionBytes | int64 | Temporary retention bytes for reset operation | No |
| retentionMs | int64 | Temporary retention time in milliseconds | No |
| restoreRetentionBytes | int64 | Retention bytes to restore after reset | No |
| restoreRetentionMs | int64 | Retention time to restore after reset | No |
| autoConfirm | bool | Whether to execute automatically without confirmation | No (defaults to false) |
| timeoutSeconds | int | Timeout for the operation in seconds | No (defaults to 300) |

### KafkaOperation Status Fields

| Field | Type | Description |
|-------|------|-------------|
| state | string | Current state of the operation |
| originalRetentionBytes | int64 | Original retention bytes before operation |
| originalRetentionMs | int64 | Original retention time in milliseconds before operation |
| currentRetentionBytes | int64 | Current retention bytes |
| currentRetentionMs | int64 | Current retention time in milliseconds |
| startTime | time | When the operation started |
| completionTime | time | When the operation completed |
| message | string | Descriptive message about the current state |
| conditions | []Condition | Detailed conditions of the operation |
| retentionReducedTime | time | When the retention was reduced (for reset operations) |

## Troubleshooting

### Common Issues

1. **Operation Stuck in Pending State**:
   - Check if the operator is running: `kubectl get pods -n <operator-namespace>`
   - Check operator logs: `kubectl logs -n <operator-namespace> <operator-pod>`

2. **Operation Failed**:
   - Check the status.message field: `kubectl get kafkaoperation <name> -o jsonpath='{.status.message}'`
   - Check operator logs for detailed error information

3. **Cannot Connect to Kafka**:
   - Verify the clusterName and clusterNamespace are correct
   - Ensure the operator has network access to the Kafka brokers
   - Check if authentication is properly configured

### Debugging

Enable verbose logging by setting the log level in the operator deployment:

```yaml
args:
  - "--zap-log-level=debug"
```

## Development

### Running Tests

```sh
# Run unit tests
make test

# Run end-to-end tests
make test-e2e
```

### Local Development

1. Install the CRDs:
   ```sh
   make install
   ```

2. Run the controller locally:
   ```sh
   make run
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Cleanup

To remove the operator and CRDs from your cluster:

```sh
# Remove the operator deployment
make undeploy

# Remove the CRDs
make uninstall
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
