using FASTERCache;

#if DEBUG
using System;

using var obj = new CacheBenchmarks();
obj.Init();
obj.FASTER_Set();
await obj.FASTER_SetAsync();
Console.WriteLine(obj.FASTER_Get());
Console.WriteLine(await obj.FASTER_GetAsync());
await obj.FASTER_SetAsyncBuffer();
Console.WriteLine(await obj.FASTER_GetAsyncBuffer());
obj.FASTER_SetBuffer();
Console.WriteLine(obj.FASTER_GetBuffer());

Console.WriteLine(obj.FASTER_GetInPlace());
Console.WriteLine(await obj.FASTER_GetAsyncInPlace());

Console.WriteLine();
obj.SQLite_Set();
await obj.SQLite_SetAsync();
Console.WriteLine(obj.SQLite_Get());
Console.WriteLine(await obj.SQLite_GetAsync());

#if REDIS
Console.WriteLine();
obj.Redis_Set();
await obj.Redis_SetAsync();
Console.WriteLine(obj.Redis_Get());
Console.WriteLine(await obj.Redis_GetAsync());
#endif

#if GARNET
Console.WriteLine();
obj.Garnet_Set();
await obj.Garnet_SetAsync();
Console.WriteLine(obj.Garnet_Get());
Console.WriteLine(await obj.Garnet_GetAsync());
#endif

#if NET8_0_OR_GREATER
Console.WriteLine();
obj.Rocks_Set();
await obj.Rocks_SetAsync();
Console.WriteLine(obj.Rocks_Get());
Console.WriteLine(await obj.Rocks_GetAsync());
#endif

Console.WriteLine();
Console.WriteLine(CacheBenchmarks.CountingBufferWriter.InstanceCount);

#else
using BenchmarkDotNet.Running;

BenchmarkRunner.Run<CacheBenchmarks>();
#endif

