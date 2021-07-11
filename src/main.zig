
const std = @import("std");

const FrontendErrors = error {
    StatementNotFound,
    StatementIncorrectSyntax,
    CommandError,
};

const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;

const StatementType = enum {
    INSERT,
    SELECT,
};

const Row = struct {
    id: u32 = 0,
    username: [COLUMN_USERNAME_SIZE]u8 = std.mem.zeroes([COLUMN_USERNAME_SIZE]u8),
    email: [COLUMN_EMAIL_SIZE]u8 = std.mem.zeroes([COLUMN_EMAIL_SIZE]u8),
};

const Statement = struct {
    type: StatementType = StatementType.SELECT,
    row: Row = .{},
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
        return FrontendErrors.CommandError;
    }
}

fn prepareStatement(line: []u8, statement: *Statement) !void {
    if (line.len < 6) {
        return FrontendErrors.StatementNotFound;
    }
    else if (std.mem.eql(u8, line[0..6], "insert")) {
        var tokens = std.mem.tokenize(line, " ");
        const initStatement = tokens.next() orelse return FrontendErrors.StatementIncorrectSyntax;
        const idRaw = tokens.next() orelse return FrontendErrors.StatementIncorrectSyntax;
        const id = std.fmt.parseInt(u32, idRaw, 10) catch |err| return FrontendErrors.StatementIncorrectSyntax;
        const username = tokens.next() orelse return FrontendErrors.StatementIncorrectSyntax;
        const email = tokens.next() orelse return FrontendErrors.StatementIncorrectSyntax;
        // If there are more elements we can return syntax error
        if (tokens.next() != null) { return FrontendErrors.StatementIncorrectSyntax; }

        statement.type = StatementType.INSERT;
        statement.row.id = id;
        std.mem.copy(u8, statement.row.username[0..], username);
        std.mem.copy(u8, statement.row.email[0..], email);
    }
    else if (std.mem.eql(u8, line[0..6], "select")) {
        statement.type = StatementType.SELECT;
    }
    else {
        // TODO: Throw and catch more specific error
        return FrontendErrors.StatementNotFound;
    }
}

fn executeStatement(stdout: anytype, statement: *Statement) !void {
    switch (statement.type) {
        StatementType.INSERT => {
            try stdout.print("Executing insert with values id {d} username {s} email {s}.\n",
                .{ statement.row.id, statement.row.username, statement.row.email });
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
            var statement: Statement = .{};
            prepareStatement(line, &statement) catch |err| {
                switch(err) {
                    FrontendErrors.StatementNotFound => {
                        try stdout.print("Unrecognized keyword at start of: {s}.\n", .{ line });
                    },
                    FrontendErrors.StatementIncorrectSyntax => {
                        try stdout.print("Incorrect syntax on statement: {s}.\n", .{ line });
                    },
                    else => unreachable,
                }
                continue;
            };

            executeStatement(&stdout, &statement) catch |err| {
                try stdout.print("Error executing command: {s}.\n", .{ line });
                continue;
            };
            try stdout.print("Executed.\n", .{});
        }
    }
}
