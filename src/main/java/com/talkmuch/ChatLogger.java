package com.talkmuch;

public class ChatLogger {
    private static final String RESET = "\u001B[0m";
    private static final String SYSTEM_COLOR = "\u001B[33m";

    private static final String[] TEXT_COLORS = {
        "\u001B[31m", // Red
        "\u001B[32m", // Green
        "\u001B[34m", // Blue
        "\u001B[35m", // Magenta
        "\u001B[36m", // Cyan
        "\u001B[91m", // Light Red
        "\u001B[92m", // Light Green
        "\u001B[94m", // Light Blue
        "\u001B[95m", // Light Magenta
        "\u001B[96m"  // Light Cyan
    };

    public static String formatMessage(String username, String text) {
        int colorIndex = Math.abs(username.hashCode()) % TEXT_COLORS.length;
        String chosenColor = TEXT_COLORS[colorIndex];
        return chosenColor + "[" + username + "]" + RESET + ": " + text;
    }

    public static void logSystem(String message) {
        System.out.println(SYSTEM_COLOR + "[System] " + message + RESET);
    }

    public static void logDisconnect(String username, String address) {
        System.out.println(SYSTEM_COLOR + "[System] User '" + username + "' (" + address + ") has disconnected." + RESET);
    }
}
