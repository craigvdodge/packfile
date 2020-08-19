using System;
using CommandLine;
//For IEnumerable
using System.Collections.Generic; 
using System.Linq;

namespace PackFile
{
    [Verb ("pack", HelpText = "Package files into a packfile.")]
    class PackOptions
    {
        [Option('s', "source", Separator=';', HelpText = "Source file(s)/director(y/ies) to add to packfile. Use semicolon (;) to seperate multiple sources.", Required = true)]
        public IEnumerable<string> Source {get; set;}

        [Option('i', "Include", Separator=';', HelpText = "Mask of files to include. Use semicolon (;) to seperate multiple masks.")]
        public IEnumerable<string> Include {get; set;}
        
        [Option('x', "Exclude", Separator=';', HelpText = "Mask of files to exclude. Use semicolon (;) to seperate multiple masks.")]
        public IEnumerable<string> Exclude {get; set;}

        [Option('f', "packfile", HelpText = "Packfile to work with.", Required = true)]
        public string PackFile {get; set;}

        [Option(HelpText = "Do not create in memory first.")]
        public bool NoMemoryBuffer {get; set;}

        [Option('c', "compression", HelpText = "Compress file data, follow with number indicating level: 0=None 1=Deflate 2=GZip 3=Brotli.", Default=0)]
        public int Compression {get; set;}

        [Option("overwrite", SetName= "Overwrite", HelpText = "Overwrite existing packfile if it exists.")]
        public bool Overwrite {get; set;}

        [Option("append", SetName="Append", HelpText = "Append to existing packfile if it exists.")]
        public bool Append {get; set;}
    }

    [Verb ("unpack", HelpText = "Extract files to a destination.")]
    class UnPackOptions
    {
        [Option('d', "destination", HelpText = "Destination for extracted files.")]
        public string Destination {get; set;}

        [Value(0, HelpText = "Packfile to work with.", Required = true)]
        public string PackFile {get; set;}
    }

    [Verb ("list", HelpText = "List files in specified packlist.")]
    class ListOptions
    {
        //[Option('f', "packfile", HelpText = "Packfile to work with", Required = true)]
        [Value(0, HelpText = "Packfile to work with.", Required = true) ]
        public string PackFile {get; set;}
    }

    class Program
    {
        public static int Main(string[] args)
        {
            return Parser.Default.ParseArguments<PackOptions, UnPackOptions, ListOptions>(args)
                .MapResult(
                    (PackOptions opts) => Pack(opts),
                    (UnPackOptions opts) => Unpack(opts),
                    (ListOptions opts) => List(opts),
                    errs => 1);
        }

        // TODO: move some of this logic back into the class
        private static int Pack(PackOptions opts)
        {
            PackFile outputFile = new PackFile();
            // Here we're relying on the commandline parser to error if overwrite AND append are specified.
            if (opts.Overwrite)
            {
                outputFile.CreationMode = PackFile.CreateMode.OverWriteExist;
            }
            if (opts.Append)
            {
                outputFile.CreationMode = PackFile.CreateMode.AppendExist;
            }

            if (opts.Compression < 0 || opts.Compression > 3)
            {
                Console.Error.WriteLine("Specified compression level {0} not supported.", opts.Compression);
                return 1;
            }

            outputFile.PackFileName = (opts.NoMemoryBuffer) ? opts.PackFile.Trim() : string.Empty;
            if ((outputFile.CreationMode == PackFile.CreateMode.AppendExist) && (System.IO.File.Exists(opts.PackFile.Trim())))
            {
                if (String.IsNullOrEmpty(outputFile.PackFileName))
                {
                    outputFile.BackupFrom(opts.PackFile.Trim());
                }
            }
            else
            {
                outputFile.Intialize();
            }

            outputFile.IncludeMasks = opts.Include.ToArray();
            outputFile.ExcludeMasks = opts.Exclude.ToArray();
            
            outputFile.Add(opts.Source, (PackFile.Compression) opts.Compression);
            if (!opts.NoMemoryBuffer)
            {
                outputFile.BackupTo(opts.PackFile.Trim());
            }
            
            return 0;
        }

        private static int Unpack(UnPackOptions opts)
        {
            PackFile packfile = new PackFile();
            packfile.PackFileName = opts.PackFile.Trim();
            string dest;
            if (String.IsNullOrEmpty(opts.Destination))
            {
                dest = System.IO.Directory.GetCurrentDirectory();
            }
            else
            {
                dest = opts.Destination.Trim();
            }
            packfile.ExtractAll(dest);
            return 0;
        }

        private static int List(ListOptions opts)
        {
            PackFile packFile = new PackFile();
            packFile.PackFileName = opts.PackFile.Trim();
            foreach (string file in packFile.List())
            Console.WriteLine(file);
            return 0;
        }
    }
}
