using System.Text.Json.Serialization;

namespace Tsg.Rdc.Model.RDCSystem;

public class RdcCallParams
{
    [JsonPropertyName("transactionId")]
    public string TransactionId { get; set; }
    
    [JsonPropertyName("receiptTransactionNumber")]
    public string ReceiptTransactionNumber { get; set; }
    
    [JsonPropertyName("accountNumber")]
    public string AccountNumber { get; set; }
    
    [JsonPropertyName("accountSuffix")]
    public string AccountSuffix { get; set; }
    
    [JsonPropertyName("amount")]
    public decimal Amount { get; set; }
    
    [JsonPropertyName("depositItemCount")]
    public int DepositItemCount { get; set; }
    
    
}