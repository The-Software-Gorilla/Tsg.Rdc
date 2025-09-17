using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Tsg.Models.Ensenta;

namespace Tsg.Rdc.Triggers.Tests;

public class DoDepositTransactionResponseTester
{
    private readonly ILogger<DepositTransaction> _logger;

    public DoDepositTransactionResponseTester(ILogger<DepositTransaction> logger)
    {
        _logger = logger;
    }

    [Function("testDoDepositResponse")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")]
        HttpRequest req)
    {

        _logger.LogInformation("C# HTTP trigger function processed a request.");
        
        string xml;
        using (var reader = new StreamReader(req.Body, Encoding.UTF8))
        {
            xml = await reader.ReadToEndAsync();
        }
        
        _logger.LogInformation(xml);
        
        // Deserialize XML to SoapEnvelope
        var serializer = new XmlSerializer(typeof(EnsentaRequestSoapEnvelope));
        EnsentaRequestSoapEnvelope envelope;
        using (var stringReader = new StringReader(xml))
        {
            envelope = (EnsentaRequestSoapEnvelope)serializer.Deserialize(stringReader);
        }
        
        // Get the DoDepositTransaction object
        var depositTransaction = envelope.Body.DoDepositTransaction;
        
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
                    TransactionId = Guid.NewGuid(),
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
            StatusCode = 200
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