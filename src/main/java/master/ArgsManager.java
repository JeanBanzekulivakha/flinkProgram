package master;

import org.apache.commons.lang3.StringUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class ArgsManager {

    private final String pathToInputFile;
    private final String pathToOutputFolder;

    public ArgsManager(String[] args) {
        System.out.printf("Start program with args: %s %n", Arrays.toString(args));

        String argPathToInputFile = args[0];
        String argPathToOutputFolder = args[1];

        validateAllPath(argPathToInputFile, argPathToOutputFolder);

        this.pathToInputFile = argPathToInputFile;
        this.pathToOutputFolder = argPathToOutputFolder;
    }

    public String getPathToInputFile() {
        return pathToInputFile;
    }

    public String getPathToOutputFolder() {
        return pathToOutputFolder;
    }

    private void validateAllPath(String argPathToInputFile, String argPathToOutputFolder) {
        if(StringUtils.isBlank(argPathToInputFile)) {
            throw new RuntimeException("Path to input file is blank or NOT provided");
        }
        if(StringUtils.isBlank(argPathToOutputFolder)) {
            throw new RuntimeException("Path to output folder is blank or NOT provided");
        }
        if(isPathNotExit(argPathToInputFile)) {
            throw new RuntimeException(String.format("Path to input file doesn't exist. " +
                    "Please check it again. Your path is: %s", argPathToInputFile));
        }
    }

    private boolean isPathNotExit(String path) {
        return !Files.exists(Path.of(path));
    }

}
