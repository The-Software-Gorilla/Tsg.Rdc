using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Tsg.Models.Ensenta;

namespace Tsg.Rdc.Triggers;

public class DepositTransaction
{
    private readonly ILogger<DepositTransaction> _logger;

    public DepositTransaction(ILogger<DepositTransaction> logger)
    {
        _logger = logger;
    }

    [Function("DepositTransaction")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequest req)
    {
        Guid transactionId = Guid.NewGuid();
        _logger.LogInformation("DoDepositTransaction received a request. {TransactionId}", transactionId);

        // Read raw SOAP/XML
        string xml;
        using (var reader = new StreamReader(req.Body, Encoding.UTF8, detectEncodingFromByteOrderMarks: true))
        {
            xml = await reader.ReadToEndAsync();
        }
        _logger.LogInformation("XML length: {Len}", xml?.Length ?? 0);
        
        var serializer = new XmlSerializer(typeof(EnsentaRequestSoapEnvelope));
        EnsentaRequestSoapEnvelope envelope;
        using (var stringReader = new StringReader(xml))
        {
            envelope = (EnsentaRequestSoapEnvelope)serializer.Deserialize(stringReader);
        }
        
        // Get the DoDepositTransaction object
        var depositTransaction = envelope.Body.DoDepositTransaction;

        // Storage connection (Azurite)
        var storageConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

        // --- Write to Table: depositTransaction ---
        var tableClient = new TableClient(storageConn, "depositTransaction");
        await tableClient.CreateIfNotExistsAsync();

        // Use a simple partition; transactionId is the RowKey ("primary key")
        var entity = new TableEntity(partitionKey: "deposit", rowKey: transactionId.ToString())
        {
            { "ReceivedUtc", DateTime.UtcNow },
            { "Status", "received" },
            { "LastUpdatedUtc", DateTime.UtcNow },
            { "Attempts", 0 },
            { "Xml", xml }   // If payloads can be huge, store in Blob instead and save a pointer here
        };
        await tableClient.AddEntityAsync(entity);

        // --- Send to Queue: deposit-inbound ---
        var queueClient = new QueueClient(storageConn, "deposit-inbound", new QueueClientOptions
        {
            MessageEncoding = QueueMessageEncoding.Base64
        });
        await queueClient.CreateIfNotExistsAsync();
        await queueClient.SendMessageAsync(transactionId.ToString());

        // Craft a response
        var responseEnvelope = new EnsentaResponseSoapEnvelope();
        responseEnvelope.Body = new EnsentaResponseSoapBody
        {
            DoDepositTransactionResponse = new DoDepositTransactionResponse
            {
                DoDepositTransactionResult = new DoDepositTransactionResult
                {
                    ResponseDescription = "Accepted",
                    ResponseCode = "00",
                    TransactionId = transactionId,
                    SystemTraceNumber = depositTransaction.ReceiptTransactionNumber,
                    TransmitZuluDateTime = DateTime.UtcNow
                    
                }
            }
        };
        
        // Serialize response to XML
        var response = ConvertToSoap(responseEnvelope);
        
        return new ContentResult
        {
            Content = response,
            ContentType = "text/xml",
            StatusCode = 202
        };
    }
    
    private string ConvertToSoap(EnsentaResponseSoapEnvelope envelope)
    {
        var serializer = new XmlSerializer(typeof(EnsentaResponseSoapEnvelope));
        string soap;
        
        var settings = new XmlWriterSettings
        {
            Encoding = new UTF8Encoding(false), // false = no BOM
            Indent = true
        };
        using (var stream = new MemoryStream())
        using (var writer = XmlWriter.Create(stream, settings))
        {
            serializer.Serialize(writer, envelope);
            soap = Encoding.UTF8.GetString(stream.ToArray());
        }
        return soap;
    }

}
