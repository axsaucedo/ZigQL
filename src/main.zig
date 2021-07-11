
const std = @import("std");

const FrontendErrors = error {
    StatementNotFound,
    StatementIncorrectSyntax,
    CommandError,
    TableFull,
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

const PAGE_SIZE: u32 = 4096;
const ROW_SIZE: u32 = @sizeOf(Row);
const ROWS_PER_PAGE: u32 = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_PAGES: u32 = 100;
const TABLE_MAX_ROWS: u32 = ROWS_PER_PAGE * TABLE_MAX_PAGES;

const Table = struct {
    numRows: u32,
    pages: [TABLE_MAX_PAGES]?[]Row,
};

const Statement = struct {
    type: StatementType = StatementType.SELECT,
    row: Row = .{},
};

// TODO: Make customizable
const MAX_INPUT_SIZE: usize = 1000;

fn saveRow(source: *Row, destination: *Row) void {
    const sourceSingSlice: *[1]Row = source;
    const sourceSlice: []Row = sourceSingSlice;
    const destinationSingSlice: *[1]Row = destination;
    const destinationSlice: []Row =  destinationSingSlice;
    std.mem.copy(Row, destinationSlice, sourceSingSlice);
}

fn loadRow(source: *Row, destination: *Row) void {
    const sourceSingSlice: *[1]Row = source;
    const sourceSlice: []Row = sourceSingSlice;
    const destinationSingSlice: *[1]Row = destination;
    const destinationSlice: []Row =  destinationSingSlice;
    std.mem.copy(Row, destinationSlice, sourceSingSlice);
}

fn rowSlot(allocator: *std.mem.Allocator, table: *Table, rowNum: u32) !*Row {
    const pageNum: u32 = rowNum / ROWS_PER_PAGE;
    // TODO: Check max age
    if (table.pages[pageNum] == null) {
        table.pages[pageNum] = try allocator.alloc(Row, ROWS_PER_PAGE);
    }
    const rowOffset: u32 = rowNum % ROWS_PER_PAGE;
    return &(table.pages[pageNum].?)[rowOffset];
}

fn newTable(allocator: *std.mem.Allocator) !*Table {
    var table: *Table = try allocator.create(Table);
    table.numRows = 0;
    for (table.pages) |_, i| {
        table.pages[i] = null;
    }
    return table;
}

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

fn executeInsert(allocator: *std.mem.Allocator, statement: *Statement, table: *Table) !void {
    if (table.numRows >= TABLE_MAX_ROWS) {
        return FrontendErrors.TableFull;
    }

    const freeRow: *Row = try rowSlot(allocator, table, table.numRows);
    saveRow(&statement.row, freeRow);
    table.numRows += 1;
}

fn printRow(stdout: anytype, row: *Row) !void {
    try stdout.print("Row: {d}, {s}, {s}\n", .{ row.id, row.username, row.email });
}

fn executeSelect(allocator: *std.mem.Allocator, stdout: anytype, statement: *Statement, table: *Table) !void {
    var row: Row = .{};
    var i: u32 = 0;
    while (i < table.numRows): (i += 1) {
        const currRow: *Row = try rowSlot(allocator, table, i);
        loadRow(currRow, &row);
        try printRow(stdout, &row);
    }
}

fn executeStatement(allocator: *std.mem.Allocator, stdout: anytype, statement: *Statement, table: *Table) !void {
    switch (statement.type) {
        StatementType.INSERT => {
            try stdout.print("Executing insert.\n", .{});
            try executeInsert(allocator, statement, table);
        },
        StatementType.SELECT => {
            try stdout.print("Executing select.\n", .{});
            try executeSelect(allocator, stdout, statement, table);
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

    const table: *Table = try newTable(pAllocator);
    defer pAllocator.destroy(table);

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

            executeStatement(pAllocator, &stdout, &statement, table) catch |err| {
                try stdout.print("Error executing command: {s}.\n", .{ line });
                continue;
            };
            try stdout.print("Executed.\n", .{});
        }
    }
}
