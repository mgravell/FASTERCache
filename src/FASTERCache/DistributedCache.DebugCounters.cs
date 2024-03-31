using Tsavorite.core;
using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTERCache;

partial class DistributedCache
{
    partial void OnDebugRMWComplete(Status status, bool async);
    partial void OnDebugRemoveComplete(Status status, bool async);
    partial void OnDebugUpsertComplete(Status status, bool async);
    partial void OnDebugFault();
    partial void DebugWipe(Span<byte> buffer);
    partial void DebugWipe(ReadOnlySpan<byte> buffer);


#if DEBUG

    partial void DebugWipe(Span<byte> buffer) => buffer.Clear();
    partial void DebugWipe(ReadOnlySpan<byte> buffer) => MemoryMarshal.CreateSpan(
        ref MemoryMarshal.GetReference(buffer), buffer.Length).Clear();

    partial void OnDebugRemoveComplete(Status status, bool async) => OnDebugComplete(status, async, false);
    partial void OnDebugUpsertComplete(Status status, bool async) => OnDebugComplete(status, async, false);
    partial void OnDebugRMWComplete(Status status, bool async) => OnDebugComplete(status, async, true);

    private void OnDebugComplete(Status status, bool async, bool isRead)
    {
        if (status.IsCompletedSuccessfully)
        {
            if (isRead)
            {
                if (status.Found && !status.Expired)
                {
                    Interlocked.Increment(ref async ? ref _asyncHit : ref _syncHit);
                }
                else if (async)
                {
                    Interlocked.Increment(ref status.Expired ? ref _asyncMissExpired : ref _asyncMissBasic);
                }
                else
                {
                    Interlocked.Increment(ref status.Expired ? ref _syncMissExpired : ref _syncMissBasic);
                }
            }
            else
            {
                Interlocked.Increment(ref async ? ref _asyncOther : ref _syncOther);
            }

            const byte StatusInPlaceUpdatedRecord = 0x20, StatusCopyUpdatedRecord = 0x30, // see Status.StatusCode internals
                StatusRMWMask = StatusInPlaceUpdatedRecord | StatusCopyUpdatedRecord;
            if ((status.Value & StatusRMWMask) == StatusCopyUpdatedRecord)
            {
                Interlocked.Increment(ref _copyUpdate);
            }
        }
        else if (status.IsFaulted)
        {
            Interlocked.Increment(ref _fault);
        }
    }
    partial void OnDebugFault() {}

    // counters are optimized to be cheap to update; read is much rarer
    private long _syncHit, _syncMissBasic, _syncMissExpired, _syncOther, _asyncHit, _asyncMissBasic, _asyncMissExpired, _asyncOther, _fault, _copyUpdate;
    
    public void ResetCounters()
    {
        Volatile.Write(ref _syncHit, 0);
        Volatile.Write(ref _syncMissBasic, 0);
        Volatile.Write(ref _syncMissExpired, 0);
        Volatile.Write(ref _syncOther, 0);
        Volatile.Write(ref _asyncHit, 0);
        Volatile.Write(ref _asyncMissBasic, 0);
        Volatile.Write(ref _asyncMissExpired, 0);
        Volatile.Write(ref _asyncOther, 0);
        Volatile.Write(ref _copyUpdate, 0);
        Volatile.Write(ref _fault, 0);
    }
    public long TotalHit => Volatile.Read(ref _syncHit) + Volatile.Read(ref _asyncHit);
    public long TotalFault => Volatile.Read(ref _fault);
    public long TotalMiss => Volatile.Read(ref _syncMissBasic) + Volatile.Read(ref _asyncMissBasic)
        + Volatile.Read(ref _syncMissExpired) + Volatile.Read(ref _asyncMissExpired);

    public long TotalSync => Volatile.Read(ref _syncHit) + Volatile.Read(ref _syncMissBasic) + Volatile.Read(ref _syncMissExpired) + Volatile.Read(ref _syncOther);

    public long TotalAsync => Volatile.Read(ref _asyncHit) + Volatile.Read(ref _asyncMissBasic) + Volatile.Read(ref _asyncMissExpired) + Volatile.Read(ref _asyncOther);
    public long TotalOther => Volatile.Read(ref _syncOther) + Volatile.Read(ref _asyncOther);
    public long TotalCopyUpdate => Volatile.Read(ref _copyUpdate);
    public long TotalMissExpired => Volatile.Read(ref _syncMissExpired) + Volatile.Read(ref _asyncMissExpired);
#endif
}
