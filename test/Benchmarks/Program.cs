#if DEBUG
using FASTERCache;
using System;

using var obj = new CacheBenchmarks();
obj.Init();
obj.FASTER_Set();
await obj.FASTER_SetAsync();
Console.WriteLine(obj.FASTER_Get());
Console.WriteLine(await obj.FASTER_GetAsync());
obj.SQLite_Set();
await obj.SQLite_SetAsync();
Console.WriteLine(obj.SQLite_Get());
Console.WriteLine(await obj.SQLite_GetAsync());
#if NET8_0_OR_GREATER
obj.Rocks_Set();
await obj.Rocks_SetAsync();
Console.WriteLine(obj.Rocks_Get());
Console.WriteLine(await obj.Rocks_GetAsync());
#endif

#else
using BenchmarkDotNet.Running;
using FASTERCache;

BenchmarkRunner.Run<CacheBenchmarks>();
#endif

