# LeadSquared Visit Plan Notifier

This is a containerized Python application designed to run as a Kubernetes CronJob. It checks LeadSquared for active sales users who have not created any "Visit Plan" tasks for the upcoming week (Monday-Saturday) and sends them an email reminder.