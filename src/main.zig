
const std = @import("std");

// TODO: Make customizable
const MAX_INPUT_SIZE: usize = 1000;

fn printPrompt() void {
    std.debug.print("db > ", .{});
}

pub fn main() !void {
    var allocatorWrapper = std.heap.GeneralPurposeAllocator(.{}){};
    //errdefer allocatorWrapper.deinit();
    const allocator = &allocatorWrapper.allocator;

    const stdout = std.io.getStdOut().writer();
    const stdin = std.io.getStdIn().reader();

    try stdout.print("Welcome to the ZigQL prompt.\n", .{});
    try stdout.print("Type your query or exit() when you're done.\n", .{});

    // Using bufferedReader for performance
    var bufReader = std.io.bufferedReader(stdin);
    const bufStream = bufReader.reader();

    while (true) {
        printPrompt();

        var readInput: ?[]u8 = bufStream.readUntilDelimiterOrEofAlloc(allocator, '\n', MAX_INPUT_SIZE) catch |err| {
            try stdout.print("Error reading as line longer than {} chars\n", .{ MAX_INPUT_SIZE });
            return;
        };
        defer allocator.free(readInput.?);

        if (std.mem.eql(u8, readInput.?, "exit()")) { break; }

        try stdout.print("Input from alloc: {s} with length {}\n", .{ readInput, readInput.?.len });
    }

    try stdout.print("Exiting ZigQL prompt.\n", .{});
}
