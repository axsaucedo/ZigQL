
const std = @import("std");

const StatementType = enum {
    INSERT,
    SELECT,
};

const Statement = struct {
    type: StatementType,
};

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

fn doMetaCommand(stdout: anytype, line: []u8) !void {
    if (std.mem.eql(u8, line, ".exit")) {
        try stdout.print("Exiting ZigQL prompt.\n", .{});
        std.process.exit(0);
    }
    else {
        // TODO: Throw and catch more specific error
        return error.CommandError;
    }
}

fn prepareStatement(line: []u8) !Statement {
    if (line.len < 6) {
        return error.CommandError;
    }
    else if (std.mem.eql(u8, line[0..6], "insert")) {
        return Statement{ .type = StatementType.INSERT };
    }
    else if (std.mem.eql(u8, line[0..6], "select")) {
        return Statement{ .type = StatementType.SELECT };
    }
    else {
        // TODO: Throw and catch more specific error
        return error.CommandError;
    }
}

fn executeStatement(stdout: anytype, statement: Statement) !void {
    switch (statement.type) {
        StatementType.INSERT => {
            try stdout.print("Executing insert.\n", .{});
        },
        StatementType.SELECT => {
            try stdout.print("Executing select.\n", .{});
        },
    }
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

        if (line[0] == '.') {
            doMetaCommand(&stdout, line) catch |err| {
                try stdout.print("Unrecognized command: {s}.\n", .{ line });
                continue;
            };
        }
        else {
            var statement: ?Statement = prepareStatement(line) catch |err| {
                try stdout.print("Unrecognized keyword at start of: {s}.\n", .{ line });
                continue;
            };

            executeStatement(&stdout, statement.?) catch |err| {
                try stdout.print("Error executing command: {s}.\n", .{ line });
                continue;
            };
            try stdout.print("Executed.\n", .{});
        }
    }
}
