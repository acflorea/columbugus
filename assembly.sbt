import AssemblyKeys._ // put this at the top of the file

assemblySettings

run in Compile <<=
  Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// your assembly settings here