# AzureEventHubClient

AzureEventHubClient is a simple, reliable, and scalable client for interacting with Azure Event Hub. 
I have added two classes, EventReceiver and EventSender, which shows a sample code of sending and receiving events in batches from event hub.

## Features

- **Event Publishing**: Send events to Azure Event Hub with minimal configuration.
- **Event Receiving**: Process events from Azure Event Hub with minimal configuration.
- **Asynchronous Processing**: Built to handle high-throughput event streaming efficiently.

## Prerequisites

Before using AzureEventHubClient, ensure you have the following:
- An **Azure Event Hub** instance.
- **Azure Storage Account** to interact with the Event Hub for checkpointing mainly, Also create a cotainer in the stroage account and update in **config.yaml**.
- A **.NET SDK** (or any language SDK) if not using this package for direct Event Hub interactions.
- Add event hub and storage account, which is used for event hub checkpointing details in **config.yaml**.

## Execution

To execute run Program.cs file from visual studio. 

## Issues?

For any error or issues please open an issues or look for previous relevant issues or email amritoit@gmail.com