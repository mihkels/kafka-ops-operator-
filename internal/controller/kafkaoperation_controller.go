/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	operationsv1alpha1 "github.io/mihkels/kafka-ops-operator/api/v1alpha1"
)

// KafkaOperationReconciler reconciles a KafkaOperation object
type KafkaOperationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the KafkaOperation object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *KafkaOperationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	operation := &operationsv1alpha1.KafkaOperation{}
	err := r.Get(ctx, req.NamespacedName, operation)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get KafkaOperation")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle based on current state
	switch operation.Status.State {
	case "": // New operation
		return r.handleNewOperation(ctx, operation, logger)
	case operationsv1alpha1.OperationStatePending:
		if operation.Spec.AutoConfirm {
			return r.startOperation(ctx, operation, logger)
		}
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	case operationsv1alpha1.OperationStateWaitingForRestore,
		operationsv1alpha1.OperationStateInProgress:
		return r.processStateChange(ctx, operation, logger)
	case operationsv1alpha1.OperationStateConfirming:
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	case operationsv1alpha1.OperationStateCompleted,
		operationsv1alpha1.OperationStateFailed,
		operationsv1alpha1.OperationStateCancelled:
		return ctrl.Result{}, nil
	default:
		logger.Info("Unknown operation state", "state", operation.Status.State)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
}

// New helper function to handle state transitions
func (r *KafkaOperationReconciler) processStateChange(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	logger.Info("Processing state change", "state", operation.Status.State)
	if operation.Status.State == operationsv1alpha1.OperationStateWaitingForRestore {
		waitDuration := operation.Spec.TimeoutSeconds * time.Second // or make this configurable
		logger.Info("Waiting for restore to complete", "timeoutSeconds", waitDuration)
		if operation.Status.RetentionReducedTime != nil && time.Since(operation.Status.RetentionReducedTime.Time) < waitDuration {
			return ctrl.Result{RequeueAfter: waitDuration - time.Since(operation.Status.RetentionReducedTime.Time)}, nil
		}

		return r.restoreTopicRetention(ctx, operation, getNamespace(operation), logger)
	}

	if operation.Status.State == operationsv1alpha1.OperationStateInProgress {
		return r.processOperation(ctx, operation, logger)
	}

	return ctrl.Result{}, nil
}

func (r *KafkaOperationReconciler) handleNewOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Initialize operation status
	operation.Status.State = operationsv1alpha1.OperationStatePending
	operation.Status.Message = "Operation pending confirmation"

	// Add condition
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Initialized",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "OperationCreated",
		Message:            "Kafka operation created and pending confirmation",
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)

	// Validate operation
	clusterNamespace := operation.Spec.ClusterNamespace
	if clusterNamespace == "" {
		clusterNamespace = operation.Namespace
	}

	// Connect to Kafka to get current retention settings
	admin, err := r.getKafkaAdminClient(ctx, operation, clusterNamespace, logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka: %v", err), logger)
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			logger.Error(err, "Failed to close the admin client")
		}
	}(admin)

	// Get current topic configuration
	topicConfig, err := admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: operation.Spec.TopicName,
	})

	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedTopicConfig",
			fmt.Sprintf("Failed to get topic configuration: %v", err), logger)
	}

	// Extract current retention settings
	for _, config := range topicConfig {
		if config.Name == "retention.bytes" {
			// Parse retention bytes
			retentionBytes, err := parseConfigInt64(config.Value, logger)
			if err != nil {
				logger.Info("Failed to parse retention bytes", "value", config.Value, "error", err)
				// Continue with zero value if parsing fails
			}

			operation.Status.OriginalRetentionBytes = retentionBytes
			operation.Status.CurrentRetentionBytes = retentionBytes
		}

		if config.Name == "retention.ms" {
			// Parse retention ms
			retentionMS, err := parseConfigInt64(config.Value, logger)
			if err != nil {
				logger.Info("Failed to parse retention ms", "value", config.Value, "error", err)
			}

			operation.Status.OriginalRetentionMS = retentionMS
			operation.Status.CurrentRetentionMS = retentionMS
		}
	}

	// Update the status
	if err := r.Status().Update(ctx, operation); err != nil {
		logger.Error(err, "Failed to update KafkaOperation status")
		return ctrl.Result{RequeueAfter: time.Second * 5}, err
	}

	// If auto-confirm is enabled, proceed directly
	if operation.Spec.AutoConfirm {
		return r.startOperation(ctx, operation, logger)
	}

	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

func (r *KafkaOperationReconciler) startOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateInProgress
	operation.Status.StartTime = &now
	operation.Status.Message = fmt.Sprintf("Starting %s operation", operation.Spec.Operation)

	// Add condition
	condition := metav1.Condition{
		Type:               "Started",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "OperationStarted",
		Message: fmt.Sprintf("Started %s operation on topic %s",
			operation.Spec.Operation, operation.Spec.TopicName),
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)

	// Update the status
	if err := r.Status().Update(ctx, operation); err != nil {
		logger.Error(err, "Failed to update KafkaOperation status")
		return ctrl.Result{RequeueAfter: time.Second * 5}, err
	}

	// Continue with processing the operation
	return r.processOperation(ctx, operation, logger)
}

func (r *KafkaOperationReconciler) processOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Execute different logic based on operation type
	switch operation.Spec.Operation {
	case operationsv1alpha1.OperationResetTopic:
		return r.executeResetTopic(ctx, operation, logger)
	default:
		return r.handleOperationError(ctx, operation, "UnsupportedOperation",
			fmt.Sprintf("Unsupported operation: %s", operation.Spec.Operation), logger)
	}
}

func (r *KafkaOperationReconciler) executeResetTopic(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	if operation.Status.CurrentRetentionBytes <= 1 { // Arbitrary small value
		logger.Info("Restoring topic retention", "topic", operation.Spec.TopicName, "originalRetention", operation.Status.OriginalRetentionBytes)
		return r.restoreTopicRetention(ctx, operation, getNamespace(operation), logger)
	}

	admin, err := r.getKafkaAdminClient(ctx, operation, getNamespace(operation), logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka: %v", err), logger)
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			logger.Error(err, "Failed to close the admin client")
		}
	}(admin)

	// Set retention bytes to 1 (smallest possible)
	retentionBytesStr := "1" // Effectively purges the topic
	configEntries := map[string]*string{
		"retention.bytes": &retentionBytesStr,
	}

	// Update the topic configuration
	if err = admin.AlterConfig(sarama.TopicResource, operation.Spec.TopicName, configEntries, false); err != nil {
		return r.handleOperationError(ctx, operation, "FailedRetentionUpdate",
			fmt.Sprintf("Failed to update topic retention: %v", err), logger)
	}

	// Update status
	now := metav1.Now()
	operation.Status.CurrentRetentionBytes = 1
	operation.Status.State = operationsv1alpha1.OperationStateWaitingForRestore
	operation.Status.Message = "Topic retention reduced, waiting before restore..."
	operation.Status.RetentionReducedTime = &now

	return r.updateStatus(ctx, operation, logger)
}

func (r *KafkaOperationReconciler) restoreTopicRetention(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, namespace string, logger logr.Logger) (ctrl.Result, error) {

	// Connect to Kafka
	admin, err := r.getKafkaAdminClient(ctx, operation, namespace, logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka during restoration: %v", err), logger)
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			logger.Error(err, "Failed to close the admin client")
		}
	}(admin)

	// Determine what retention to restore to
	configEntries := make(map[string]*string)

	// Handle retention bytes
	restoreRetentionBytes := operation.Status.OriginalRetentionBytes
	if operation.Spec.RestoreRetentionBytes != 0 {
		restoreRetentionBytes = operation.Spec.RestoreRetentionBytes
	}
	retentionBytesStr := fmt.Sprintf("%d", restoreRetentionBytes)
	configEntries["retention.bytes"] = &retentionBytesStr

	// Handle retention MS if specified
	if operation.Spec.RestoreRetentionMS != 0 || operation.Status.OriginalRetentionMS != 0 {
		restoreRetentionMS := operation.Status.OriginalRetentionMS
		if operation.Spec.RestoreRetentionMS != 0 {
			restoreRetentionMS = operation.Spec.RestoreRetentionMS
		}
		retentionMSStr := fmt.Sprintf("%d", restoreRetentionMS)
		configEntries["retention.ms"] = &retentionMSStr
	}

	// Update the topic configuration
	if err = admin.AlterConfig(sarama.TopicResource, operation.Spec.TopicName, configEntries, false); err != nil {
		return r.handleOperationError(ctx, operation, "FailedRetentionRestore",
			fmt.Sprintf("Failed to restore topic retention: %v", err), logger)
	}

	// Complete the operation
	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateCompleted
	operation.Status.CompletionTime = &now
	operation.Status.CurrentRetentionBytes = restoreRetentionBytes
	operation.Status.Message = "Topic reset completed successfully, retention restored"

	// Add condition
	addCondition(operation, "Completed", "OperationCompleted",
		"Topic reset operation completed successfully")

	return r.updateStatus(ctx, operation, logger)
}

func (r *KafkaOperationReconciler) handleOperationError(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, reason, message string, log logr.Logger) (ctrl.Result, error) {
	log.Error(fmt.Errorf(message), "Operation failed", "reason", reason)

	// Update status
	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateFailed
	operation.Status.CompletionTime = &now
	operation.Status.Message = message

	// Add condition
	condition := metav1.Condition{
		Type:               "Failed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)

	// Update the status
	if err := r.Status().Update(ctx, operation); err != nil {
		log.Error(err, "Failed to update KafkaOperation status for failure")
		return ctrl.Result{RequeueAfter: time.Second * 5}, err
	}

	return ctrl.Result{}, nil
}

func (r *KafkaOperationReconciler) getKafkaAdminClient(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, namespace string, logger logr.Logger) (sarama.ClusterAdmin, error) {
	// Create Sarama configuration
	config := sarama.NewConfig()
	config.Version = sarama.V3_8_1_0 // Use appropriate Kafka version

	// Get bootstrap servers from Strimzi CR or Secret
	bootstrapServers := []string{fmt.Sprintf("%s-kafka-bootstrap.%s:9092",
		operation.Spec.ClusterName,
		namespace),
	}

	logger.Info("Bootstrap servers", "servers", bootstrapServers)

	// Create the admin client
	admin, err := sarama.NewClusterAdmin(bootstrapServers, config)
	if err != nil {
		logger.Error(err, "Failed to create Kafka admin client")
		return nil, err
	}

	return admin, nil
}

// Helper function to get namespace
func getNamespace(operation *operationsv1alpha1.KafkaOperation) string {
	if operation.Spec.ClusterNamespace != "" {
		return operation.Spec.ClusterNamespace
	}
	return operation.Namespace
}

// Helper function to add a condition
func addCondition(operation *operationsv1alpha1.KafkaOperation, condType, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)
}

// Helper function for status updates
func (r *KafkaOperationReconciler) updateStatus(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	logger.Info("Updating KafkaOperation status", "state", operation.Status.State)
	if err := r.Status().Update(ctx, operation); err != nil {
		logger.Error(err, "Failed to update KafkaOperation status")
		return ctrl.Result{RequeueAfter: time.Second * 5}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KafkaOperationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operationsv1alpha1.KafkaOperation{}).
		Named("kafkaoperation").
		Complete(r)
}

func parseConfigInt64(value string, logger logr.Logger) (int64, error) {
	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		return 0, fmt.Errorf("failed to parse value '%s' to int64: %w", value, err)
	}
	return result, nil
}
