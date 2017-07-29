namespace KafkaClient.Protocol
{
    /// <summary>
    /// This is for controlling the visibility of transactional records.
    /// </summary>
    public static class IsolationLevels
    {
        /// <summary>
        /// Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible.
        /// </summary>
        public const byte ReadUnCommitted = 0;

        /// <summary>
        /// With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. 
        /// To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables 
        /// the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records.
        /// </summary>
        public const byte ReadCommitted = 1;
    }
}