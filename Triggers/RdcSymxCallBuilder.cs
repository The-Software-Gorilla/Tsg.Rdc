using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Tsg.Rdc.Model.RDCSystem;
using Tsg.Models.SymX;

namespace Tsg.Rdc.Triggers;

public class RdcSymxCallBuilder
{
    private readonly ILogger<RdcSymxCallBuilder> _logger;
    private readonly string _storageConn;
    private readonly string _symxPowerOn;
    private readonly string _symxInstanceUrl;
    private readonly string _symxUser;
    private readonly string _symxDeviceType;
    private readonly int _symxDeviceNumber;
    private const string RdcCallbackQueue = "deposit-callback";
    private const string SymxOutboundQueue = "symx-outbound";
    private const string SymxTableName = "symxOutbound";
    private const string SymxPartitionKey = "symxCall";
    private const string DepositTableName = "depositTransaction";
    private const string DepositPartitionKey = "deposit";
    


    public RdcSymxCallBuilder(ILogger<RdcSymxCallBuilder> logger)
    {
        _logger = logger;
        _storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage")
                       ?? throw new InvalidOperationException("AzureWebJobsStorage is not set.");
        _symxPowerOn = Environment.GetEnvironmentVariable("SymxPowerOn")
                       ?? throw new InvalidOperationException("SymxPowerOn is not set.");
        _symxInstanceUrl = Environment.GetEnvironmentVariable("SymxInstanceUrl")
                       ?? throw new InvalidOperationException("SymxInstanceUrl is not set.");
        _symxUser = Environment.GetEnvironmentVariable("SymxUser")
                       ?? throw new InvalidOperationException("SymxUser is not set.");
        _symxDeviceType = Environment.GetEnvironmentVariable("SymxDeviceType")
                       ?? throw new InvalidOperationException("SymxDeviceType is not set.");
        var deviceNumberStr = Environment.GetEnvironmentVariable("SymxDeviceNumber")
                       ?? throw new InvalidOperationException("SymxDeviceNumber is not set.");
        if (!int.TryParse(deviceNumberStr, out _symxDeviceNumber))
        {
            throw new InvalidOperationException("SymxDeviceNumber is not a valid integer.");
        }
    }

    [Function(nameof(RdcSymxCallBuilder))]
    public async Task Run([QueueTrigger("deposit-forward", Connection = "AzureWebJobsStorage")] QueueMessage message)
    {
        _logger.LogInformation(message.MessageText);
        
        var rdcCallParams = JsonSerializer.Deserialize<RdcCallParams>(message.MessageText);

        var rdcSymxEnvelope = CreateRdcSymXEnvelope(rdcCallParams);
        
        var symXCall = new SymXCall
        {
            SymXCallId = Guid.NewGuid().ToString(),
            CorrelationId = rdcCallParams.TransactionId,
            CallbackQueue = RdcCallbackQueue,
            SymXInstanceUrl = _symxInstanceUrl,
            SymXPowerOn = _symxPowerOn,
            SymXEnvelope = rdcSymxEnvelope
        };
        
        var json = JsonSerializer.Serialize(symXCall, new JsonSerializerOptions { WriteIndented = true });
        
        await StoreSymxCallInTable(symXCall, rdcCallParams, json);
        
        await QueueSymxCall(symXCall.SymXCallId);
        
        await UpdateDepositEntityStatus(rdcCallParams.TransactionId, "symx_queued");
        
        _logger.LogInformation("RDC SymX Call XML for Transaction ID: {TransactionId}, SymXCallId: {CallId}", rdcCallParams.TransactionId, symXCall.SymXCallId);
        
    }
    
    private async Task UpdateDepositEntityStatus(string transactionId, string status)
    {
        var depositTable = new TableClient(_storageConn, DepositTableName);
        try
        {
            var resp = await depositTable.GetEntityAsync<TableEntity>(DepositPartitionKey, transactionId);
            var depositEntity = resp.Value;
            depositEntity["Status"] = status;
            depositEntity["LastUpdatedUtc"] = DateTime.UtcNow;
            await depositTable.UpdateEntityAsync(depositEntity, depositEntity.ETag, TableUpdateMode.Replace);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            _logger.LogError(ex, "No deposit transaction entity found for PK={PK} RK={RK}", DepositPartitionKey, transactionId);
        }
    }
    
    private async Task QueueSymxCall(string symXCallId)
    {
        var queueClient = new QueueClient(_storageConn, SymxOutboundQueue, new QueueClientOptions
        {
            MessageEncoding = QueueMessageEncoding.Base64
        });
        await queueClient.CreateIfNotExistsAsync();
        await queueClient.SendMessageAsync(symXCallId);
        
    }
    
    private async Task StoreSymxCallInTable(SymXCall symXCall, RdcCallParams rdcCallParams, string json)
    {
        var symxTable = new TableClient(_storageConn, SymxTableName);
        await symxTable.CreateIfNotExistsAsync();
        var symxEntity = new TableEntity(SymxPartitionKey, symXCall.SymXCallId)
        {
            { "CorrelationId", rdcCallParams.TransactionId },
            { "ReceivedUtc", DateTime.UtcNow },
            { "Status", "created" },
            { "LastUpdatedUtc", DateTime.UtcNow },
            { "Attempts", 0 },
            { "Call", json } // If payloads can be huge, store in Blob instead and save a pointer here
        };
        await symxTable.AddEntityAsync(symxEntity);
    }
    
    private SymXSoapEnvelope CreateRdcSymXEnvelope(RdcCallParams rdcCallParams)
    {
        return new SymXSoapEnvelope
        {
            Header = new SymXSoapHeader
            {
            },
            Body = new SymXSoapBody
            {
                ExecutePowerOnReturnArray = new ExecutePowerOnReturnArray
                {
                    Request = new Request
                    {
                        BranchId = 0,
                        Credentials = new Credentials
                        {
                            ProcessorUser = _symxUser,
                            AdministrativeCredentials = new AdministrativeCredentials()
                            {
                                Password = "PASSWORD"
                            }
                        },
                        DeviceInformation = new DeviceInformation
                        {
                            DeviceType = _symxDeviceType,
                            DeviceNumber = _symxDeviceNumber
                        },
                        Header = new RequestHeader
                        {
                            MessageId = rdcCallParams.ReceiptTransactionNumber
                        },
                        Body = new RequestBody
                        {
                            File = _symxPowerOn,
                            RgSession = 1,
                            UserDefinedParameters = new UserDefinedParameters
                            {
                                RgUserChr =
                                [
                                    new RgUserChr
                                    {
                                        Id = 1,
                                        Value = rdcCallParams.AccountNumber
                                    },

                                    new RgUserChr
                                    {
                                        Id = 2,
                                        Value = rdcCallParams.AccountSuffix
                                    },

                                    new RgUserChr
                                    {
                                        Id = 3,
                                        Value = rdcCallParams.ReceiptTransactionNumber
                                    }
                                ],
                                RgUserNum =
                                [
                                    new RgUserNum
                                    {
                                        Id = 1,
                                        Value = (int)(rdcCallParams.Amount * 100) // Amount in cents
                                    }
                                ]
                            },
                            User = _symxUser
                        }
                    }
                }
            }
        };
    }
}