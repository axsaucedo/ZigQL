
const std = @import("std");

// TODO: Make customizable
const MAX_INPUT_SIZE: usize = 1000;

fn printPrompt() void {
    std.debug.print("db > ", .{});
}

fn readInput(bufStream: anytype, allocator: *std.mem.Allocator) []u8 {
    var line: ?[]u8 = bufStream.readUntilDelimiterOrEofAlloc(allocator, '\n', MAX_INPUT_SIZE) catch |err| {
        return "";
    };
    return line.?;
}

pub fn main() !void {
    var allocatorWrapper = std.heap.GeneralPurposeAllocator(.{}){};
    //errdefer allocatorWrapper.deinit();
    const pAllocator = &allocatorWrapper.allocator;

    const stdout = std.io.getStdOut().writer();
    const stdin = std.io.getStdIn().reader();

    try stdout.print("Welcome to the ZigQL prompt.\n", .{});
    try stdout.print("Type your query or .exit when you're done.\n", .{});

    // Using bufferedReader for performance
    var bufReader = std.io.bufferedReader(stdin);
    const pBufStream = &bufReader.reader();

    while (true) {
        printPrompt();

        var line: []u8 = readInput(pBufStream, pAllocator);
        defer pAllocator.free(line);

        if (std.mem.eql(u8, line, ".exit")) { break; }

        try stdout.print("Input from alloc: {s} with length {}\n", .{ line, line.len });
    }

    try stdout.print("Exiting ZigQL prompt.\n", .{});
}
