﻿using System;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public class SslTransport : ITransport
    {
        private Socket _tcpSocket;
        private Stream _stream;
        private readonly ReconnectingSocket _socket;

        private readonly Endpoint _endpoint;
        private readonly IConnectionConfiguration _configuration;
        private readonly ISslConfiguration _sslConfiguration;
        private readonly ILog _log;

        private int _disposeCount; // = 0;
        private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _readSemaphore = new SemaphoreSlim(1, 1);

        public SslTransport(Endpoint endpoint, IConnectionConfiguration configuration, ILog log)
        {
            if (configuration?.SslConfiguration == null) throw new ArgumentOutOfRangeException(nameof(configuration), "Must have SslConfiguration set");
            _sslConfiguration = configuration.SslConfiguration;

            _endpoint = endpoint;
            _configuration = configuration;
            _log = log;
            _socket = new ReconnectingSocket(endpoint, configuration, log, true);
        }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            var socket = await _socket.ConnectAsync(cancellationToken);
            if (ReferenceEquals(_tcpSocket, socket)) return;

            Interlocked.Exchange(ref _stream, null)?.Dispose();
            try {
                _stream = new NetworkStream(socket, true);
                var sslStream = new SslStream(
                    _stream,
                    false,
                    _sslConfiguration.RemoteCertificateValidationCallback,
                    _sslConfiguration.LocalCertificateSelectionCallback,
                    _sslConfiguration.EncryptionPolicy
                );
                _stream = sslStream;
                _log.Verbose(() => LogEvent.Create($"Attempting SSL connection to {_endpoint.Host}, SslProtocol:{_sslConfiguration.EnabledProtocols}, Policy:{_sslConfiguration.EncryptionPolicy}"));
                await sslStream.AuthenticateAsClientAsync(_endpoint.Host, _sslConfiguration.LocalCertificates, _sslConfiguration.EnabledProtocols, _sslConfiguration.CheckCertificateRevocation).ThrowIfCancellationRequested(cancellationToken).ConfigureAwait(false);
                _stream = sslStream;
                _log.Info(() => LogEvent.Create($"Successful SSL connection, SslProtocol:{sslStream.SslProtocol}, KeyExchange:{sslStream.KeyExchangeAlgorithm}.{sslStream.KeyExchangeStrength}, Cipher:{sslStream.CipherAlgorithm}.{sslStream.CipherStrength}, Hash:{sslStream.HashAlgorithm}.{sslStream.HashStrength}, Authenticated:{sslStream.IsAuthenticated}, MutuallyAuthenticated:{sslStream.IsMutuallyAuthenticated}, Encrypted:{sslStream.IsEncrypted}, Signed:{sslStream.IsSigned}"));
                _tcpSocket = socket;
            } catch (Exception ex) {
                _log.Warn(() => LogEvent.Create(ex, "SSL connection failed"));
                Interlocked.Exchange(ref _stream, null)?.Dispose();
            }
        }

        public async Task<int> ReadBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            var totalBytesRead = 0;
            try {
                await _readSemaphore.LockAsync( // serialize receiving on a given transport
                    async () => {
                        var stream = _stream; // so if the socket is reconnected, we don't read partially from different sockets
                        var socket = _tcpSocket;
                        _configuration.OnReading?.Invoke(_endpoint, buffer.Count);
                        timer.Start();
                        while (totalBytesRead < buffer.Count && !cancellationToken.IsCancellationRequested) {
                            var bytesRemaining = buffer.Count - totalBytesRead;
                            _log.Verbose(() => LogEvent.Create($"Reading ({bytesRemaining}? bytes) from {_endpoint}"));
                            _configuration.OnReadingBytes?.Invoke(_endpoint, bytesRemaining);
                            var bytesRead = await stream.ReadAsync(buffer.Array, buffer.Offset + totalBytesRead, bytesRemaining, cancellationToken).ConfigureAwait(false);
                            totalBytesRead += bytesRead;
                            _configuration.OnReadBytes?.Invoke(_endpoint, bytesRemaining, bytesRead, timer.Elapsed);
                            _log.Verbose(() => LogEvent.Create($"Read {bytesRead} bytes from {_endpoint}"));

                            if (bytesRead <= 0 && socket.Available == 0) {
                                _socket.Disconnect(socket);
                                var ex = new ConnectionException(_endpoint);
                                _configuration.OnDisconnected?.Invoke(_endpoint, ex);
                                throw ex;
                            }
                        }
                        timer.Stop();
                        _configuration.OnRead?.Invoke(_endpoint, totalBytesRead, timer.Elapsed);
                    }, cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) {
                timer.Stop();
                _configuration.OnReadFailed?.Invoke(_endpoint, buffer.Count, timer.Elapsed, ex);
                if (_disposeCount > 0) throw new ObjectDisposedException(nameof(SslTransport));
                throw;
            }
            return totalBytesRead;
        }

        public async Task<int> WriteBytesAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken, int correlationId = 0)
        {
            var totalBytes = buffer.Count;
            await _writeSemaphore.LockAsync( // serialize sending on a given transport
                async () => {
                    var stream = _stream; // so if the socket is reconnected, we don't write partially to different sockets
                    var timer = Stopwatch.StartNew();
                    cancellationToken.ThrowIfCancellationRequested();
                
                    _log.Verbose(() => LogEvent.Create($"Writing {totalBytes}? bytes (id {correlationId}) to {_endpoint}"));
                    _configuration.OnWritingBytes?.Invoke(_endpoint, totalBytes);
                    await stream.WriteAsync(buffer.Array, buffer.Offset, totalBytes, cancellationToken).ConfigureAwait(false);
                    _configuration.OnWroteBytes?.Invoke(_endpoint, totalBytes, totalBytes, timer.Elapsed);
                    _log.Verbose(() => LogEvent.Create($"Wrote {totalBytes} bytes (id {correlationId}) to {_endpoint}"));
                }, cancellationToken).ConfigureAwait(false);
            return totalBytes;
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _writeSemaphore.Dispose();
            _stream?.Dispose();
            _socket.Dispose();
        }
    }
}
