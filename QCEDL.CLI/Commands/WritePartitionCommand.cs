using System.CommandLine;
using System.Diagnostics;
using QCEDL.CLI.Core;
using QCEDL.CLI.Helpers;
using Qualcomm.EmergencyDownload.Layers.APSS.Firehose;
using Qualcomm.EmergencyDownload.Layers.APSS.Firehose.Xml.Elements;

namespace QCEDL.CLI.Commands;

internal sealed class WritePartitionCommand
{
    private static readonly Argument<string> PartitionNameArgument =
        new("partition_name", "The name of the partition to write.");

    private static readonly Argument<FileInfo> FilenameArgument =
        new("filename", "The file containing data to write to the partition.")
        {
            Arity = ArgumentArity.ExactlyOne
        };

    private static readonly Option<uint?> LunOption = new(
        aliases: ["--lun", "-u"],
        description: "Specify the LUN number. If not specified, all LUNs will be scanned for the partition.");

    public static Command Create(GlobalOptionsBinder globalOptionsBinder)
    {
        var command = new Command("write-part", "Writes data from a file to a partition by name.")
        {
            PartitionNameArgument, FilenameArgument, LunOption
        };

        _ = FilenameArgument.ExistingOnly();

        command.SetHandler(ExecuteAsync,
            globalOptionsBinder,
            PartitionNameArgument,
            FilenameArgument,
            LunOption);

        return command;
    }

    private static async Task<int> ExecuteAsync(
        GlobalOptionsBinder globalOptions,
        string partitionName,
        FileInfo inputFile,
        uint? specifiedLun)
    {
        Logging.Log($"Executing 'write-part' command: Partition '{partitionName}', File '{inputFile.FullName}'...", LogLevel.Trace);
        var commandStopwatch = Stopwatch.StartNew();

        if (!inputFile.Exists)
        {
            Logging.Log($"Error: Input file '{inputFile.FullName}' not found.", LogLevel.Error);
            return 1;
        }

        if (inputFile.Length == 0)
        {
            Logging.Log("Error: Input file is empty. Nothing to write.", LogLevel.Error);
            return 1;
        }

        try
        {
            using var manager = new EdlManager(globalOptions);

            return manager.IsHostDeviceMode
                ? await ExecuteHostDeviceModeAsync(manager, partitionName, inputFile, specifiedLun)
                : await ExecuteFirehoseModeAsync(manager, globalOptions, partitionName, inputFile, specifiedLun);
        }
        catch (FileNotFoundException ex)
        {
            Logging.Log(ex.Message, LogLevel.Error);
            return 1;
        }
        catch (ArgumentException ex)
        {
            Logging.Log(ex.Message, LogLevel.Error);
            return 1;
        }
        catch (IOException ex)
        {
            Logging.Log($"IO Error (e.g., reading input file): {ex.Message}", LogLevel.Error);
            return 1;
        }
        catch (PlatformNotSupportedException ex)
        {
            Logging.Log($"Platform Error: {ex.Message}", LogLevel.Error);
            return 1;
        }
        catch (Exception ex)
        {
            Logging.Log($"An unexpected error occurred in 'write-part': {ex.Message}", LogLevel.Error);
            Logging.Log(ex.ToString(), LogLevel.Debug);
            return 1;
        }
        finally
        {
            commandStopwatch.Stop();
            Logging.Log($"'write-part' command finished in {commandStopwatch.Elapsed.TotalSeconds:F2}s.", LogLevel.Debug);
        }
    }

    private static async Task<int> ExecuteHostDeviceModeAsync(
        EdlManager manager,
        string partitionName,
        FileInfo inputFile,
        uint? specifiedLun)
    {
        Logging.Log("Operating in host device mode (direct MTD access)", LogLevel.Info);

        if (specifiedLun.HasValue && specifiedLun.Value != 0)
        {
            Logging.Log("Warning: LUN parameter is ignored in host device mode.", LogLevel.Warning);
        }

        // Find the partition
        Logging.Log($"Searching for partition '{partitionName}' on host device...", LogLevel.Debug);
        var foundPartition = await manager.FindPartitionAsync(partitionName);

        if (!foundPartition.HasValue)
        {
            Logging.Log($"Error: Partition '{partitionName}' not found on host device.", LogLevel.Error);
            return 1;
        }

        var hostManager = manager.GetHostDeviceManager();
        var sectorSize = hostManager.SectorSize;
        var partition = foundPartition.Value;

        var partitionSizeInBytes = (long)(partition.LastLBA - partition.FirstLBA + 1) * sectorSize;
        var originalFileLength = inputFile.Length;

        Logging.Log($"Found partition '{partitionName}': LBA {partition.FirstLBA}-{partition.LastLBA}, Size: {partitionSizeInBytes / 1024.0 / 1024.0:F2} MiB");

        if (originalFileLength > partitionSizeInBytes)
        {
            Logging.Log($"Error: Input file size ({originalFileLength} bytes) is larger than partition '{partitionName}' size ({partitionSizeInBytes} bytes).", LogLevel.Error);
            return 1;
        }

        // Read the input file
        byte[] inputData;
        try
        {
            inputData = await File.ReadAllBytesAsync(inputFile.FullName);
        }
        catch (IOException ioEx)
        {
            Logging.Log($"Error reading input file '{inputFile.FullName}': {ioEx.Message}", LogLevel.Error);
            return 1;
        }

        var remainder = originalFileLength % sectorSize;
        if (remainder != 0)
        {
            var paddedSize = originalFileLength + (sectorSize - remainder);
            Logging.Log($"Input file size ({originalFileLength} bytes) is not a multiple of sector size ({sectorSize} bytes). Will pad with zeros to {paddedSize} bytes.", LogLevel.Info);
        }

        if (originalFileLength < partitionSizeInBytes)
        {
            Logging.Log($"Warning: Input data size ({originalFileLength} bytes) is smaller than partition size ({partitionSizeInBytes} bytes). Only the first part of the partition will be written.", LogLevel.Warning);
        }

        Logging.Log($"Writing {originalFileLength} bytes to partition '{partitionName}' on host device...");

        long bytesWrittenReported = 0;
        var writeStopwatch = Stopwatch.StartNew();

        void ProgressAction(long current, long total)
        {
            bytesWrittenReported = current;
            var percentage = total == 0 ? 100 : current * 100.0 / total;
            var elapsed = writeStopwatch.Elapsed;
            var speed = current / elapsed.TotalSeconds;
            var speedStr = "N/A";
            if (elapsed.TotalSeconds > 0.1)
            {
                speedStr = speed > 1024 * 1024 ? $"{speed / (1024 * 1024):F2} MiB/s" :
                    speed > 1024 ? $"{speed / 1024:F2} KiB/s" :
                    $"{speed:F0} B/s";
            }
            Console.Write($"\rWriting: {percentage:F1}% ({current / (1024.0 * 1024.0):F2} / {total / (1024.0 * 1024.0):F2} MiB) [{speedStr}]      ");
        }

        try
        {
            writeStopwatch.Start();
            await Task.Run(() => hostManager.WritePartition(partitionName, inputData, ProgressAction));
            writeStopwatch.Stop();
        }
        catch (Exception ex)
        {
            Logging.Log($"Error during host device partition write: {ex.Message}", LogLevel.Error);
            Console.WriteLine(); // Clear progress line
            return 1;
        }

        Console.WriteLine(); // Newline after progress bar

        Logging.Log($"Successfully wrote {bytesWrittenReported / (1024.0 * 1024.0):F2} MiB to partition '{partitionName}' in {writeStopwatch.Elapsed.TotalSeconds:F2}s.");
        return 0;
    }

    private static async Task<int> ExecuteFirehoseModeAsync(
        EdlManager manager,
        GlobalOptionsBinder globalOptions,
        string partitionName,
        FileInfo inputFile,
        uint? specifiedLun)
    {
        var foundPartitionInfo = await manager.FindPartitionWithLunAsync(partitionName, specifiedLun);

        if (!foundPartitionInfo.HasValue)
        {
            Logging.Log($"Error: Partition '{partitionName}' not found on " +
                       (specifiedLun.HasValue ? $"LUN {specifiedLun.Value}." : "any scanned LUN."), LogLevel.Error);
            return 1;
        }

        var (foundPartition, actualLun) = foundPartitionInfo.Value;
        var storageType = globalOptions.MemoryType ?? StorageType.Ufs;

        // Get sector size for the LUN where partition was found
        uint actualSectorSize;
        try
        {
            var lunStorageInfo = await Task.Run(() => manager.Firehose.GetStorageInfo(storageType, actualLun, globalOptions.Slot));
            actualSectorSize = lunStorageInfo?.StorageInfo?.BlockSize > 0 ? (uint)lunStorageInfo.StorageInfo.BlockSize : 0;

            if (actualSectorSize == 0)
            {
                actualSectorSize = storageType switch
                {
                    StorageType.Nvme => 512,
                    StorageType.Sdcc => 512,
                    StorageType.Spinor or StorageType.Ufs or StorageType.Nand or _ => 4096,
                };
                Logging.Log($"Storage info for LUN {actualLun} unreliable, using default sector size for {storageType}: {actualSectorSize}", LogLevel.Warning);
            }
        }
        catch (Exception storageEx)
        {
            Logging.Log($"Could not get storage info for LUN {actualLun}. Error: {storageEx.Message}. Using default sector size.", LogLevel.Warning);
            actualSectorSize = storageType switch
            {
                StorageType.Nvme => 512,
                StorageType.Sdcc => 512,
                StorageType.Spinor or StorageType.Ufs or StorageType.Nand or _ => 4096,
            };
        }

        var originalFileLength = inputFile.Length;
        long totalBytesToWriteIncludingPadding;
        uint numSectorsForXml;

        var partitionSizeInBytes = (long)(foundPartition.LastLBA - foundPartition.FirstLBA + 1) * actualSectorSize;

        if (originalFileLength > partitionSizeInBytes)
        {
            Logging.Log($"Error: Input file size ({originalFileLength} bytes) is larger than the partition '{partitionName}' size ({partitionSizeInBytes} bytes).", LogLevel.Error);
            return 1;
        }

        var remainder = originalFileLength % actualSectorSize;
        if (remainder != 0)
        {
            totalBytesToWriteIncludingPadding = originalFileLength + (actualSectorSize - remainder);
            Logging.Log($"Input file size ({originalFileLength} bytes) is not a multiple of partition's sector size ({actualSectorSize} bytes). Will pad with zeros to {totalBytesToWriteIncludingPadding} bytes.", LogLevel.Warning);
        }
        else
        {
            totalBytesToWriteIncludingPadding = originalFileLength;
        }

        if (totalBytesToWriteIncludingPadding > partitionSizeInBytes)
        {
            Logging.Log($"Error: Padded data size ({totalBytesToWriteIncludingPadding} bytes) would be larger than the partition '{partitionName}' size ({partitionSizeInBytes} bytes). This should not happen if original file fits.", LogLevel.Error);
            return 1;
        }

        numSectorsForXml = (uint)(totalBytesToWriteIncludingPadding / actualSectorSize);

        Logging.Log($"Data to write: {originalFileLength} bytes from file, padded to {totalBytesToWriteIncludingPadding} bytes ({numSectorsForXml} sectors).", LogLevel.Debug);
        if (totalBytesToWriteIncludingPadding < partitionSizeInBytes)
        {
            Logging.Log($"Warning: Padded data size is smaller than partition size. The remaining space in partition '{partitionName}' will not be explicitly overwritten or zeroed out by this operation beyond the {totalBytesToWriteIncludingPadding} bytes written.", LogLevel.Warning);
        }

        var partStartSector = foundPartition.FirstLBA;
        if (partStartSector > uint.MaxValue)
        {
            Logging.Log($"Error: Partition start LBA ({partStartSector}) exceeds uint.MaxValue, not supported by current Firehose.ProgramFromStream.", LogLevel.Error);
            return 1;
        }

        Logging.Log($"Attempting to write {totalBytesToWriteIncludingPadding} bytes to partition '{partitionName}' (LUN {actualLun}, LBA {partStartSector})...");

        long bytesWrittenReported = 0;
        var writeStopwatch = new Stopwatch();

        void ProgressAction(long current, long total)
        {
            bytesWrittenReported = current;
            var percentage = total == 0 ? 100 : current * 100.0 / total;
            var elapsed = writeStopwatch.Elapsed;
            var speed = current / elapsed.TotalSeconds;
            var speedStr = "N/A";
            if (elapsed.TotalSeconds > 0.1)
            {
                speedStr = speed > 1024 * 1024 ? $"{speed / (1024 * 1024):F2} MiB/s" :
                    speed > 1024 ? $"{speed / 1024:F2} KiB/s" :
                    $"{speed:F0} B/s";
            }

            Console.Write($"\rWriting: {percentage:F1}% ({current / (1024.0 * 1024.0):F2} / {total / (1024.0 * 1024.0):F2} MiB) [{speedStr}]      ");
        }

        bool success;
        try
        {
            using var fileStream = inputFile.OpenRead();

            writeStopwatch.Start();
            success = await Task.Run(() => manager.Firehose.ProgramFromStream(
                storageType,
                actualLun,
                globalOptions.Slot,
                actualSectorSize,
                (uint)partStartSector,
                numSectorsForXml,
                totalBytesToWriteIncludingPadding,
                inputFile.Name,
                fileStream,
                ProgressAction
            ));
            writeStopwatch.Stop();
        }
        catch (IOException ioEx)
        {
            Logging.Log($"IO Error reading input file '{inputFile.FullName}': {ioEx.Message}", LogLevel.Error);
            Console.WriteLine();
            return 1;
        }

        Console.WriteLine(); // Newline after progress bar

        if (success)
        {
            Logging.Log($"Data ({bytesWrittenReported / (1024.0 * 1024.0):F2} MiB) successfully written to partition '{partitionName}' in {writeStopwatch.Elapsed.TotalSeconds:F2}s.");
        }
        else
        {
            Logging.Log($"Failed to write data to partition '{partitionName}'. Check previous logs.", LogLevel.Error);
            return 1;
        }

        return 0;
    }
}