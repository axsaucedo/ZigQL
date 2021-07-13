
const std = @import("std");

const ZqlError = error {
    StatementNotFound,
    StatementIncorrectSyntax,
    CommandError,
    TableFull,
    StatementParamTooLong,
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

fn printPrompt(stdout: std.fs.File.Writer) !void {
    try stdout.print("db > ", .{});
}

fn readInput(bufStream: anytype, allocator: *std.mem.Allocator) []u8 {
    var line: ?[]u8 = bufStream.readUntilDelimiterOrEofAlloc(allocator, '\n', MAX_INPUT_SIZE) catch |err| {
        return "";
    };
    return line.?;
}

fn doMetaCommand(stdout: std.fs.File.Writer, line: []u8) !void {
    if (std.mem.eql(u8, line, ".exit")) {
        try stdout.print("Exiting ZigQL prompt.\n", .{});
        std.process.exit(0);
    }
    else {
        // TODO: Throw and catch more specific error
        return ZqlError.CommandError;
    }
}

fn prepareStatement(line: []u8, statement: *Statement) !void {
    if (line.len < 6) {
        return ZqlError.StatementNotFound;
    }
    else if (std.mem.eql(u8, line[0..6], "insert")) {
        var tokens = std.mem.tokenize(line, " ");
        const initStatement = tokens.next() orelse return ZqlError.StatementIncorrectSyntax;
        const idRaw = tokens.next() orelse return ZqlError.StatementIncorrectSyntax;
        const id = std.fmt.parseInt(u32, idRaw, 10) catch |err| return ZqlError.StatementIncorrectSyntax;
        const username = tokens.next() orelse return ZqlError.StatementIncorrectSyntax;
        if (username.len > COLUMN_USERNAME_SIZE) {
            return ZqlError.StatementParamTooLong;
        }
        const email = tokens.next() orelse return ZqlError.StatementIncorrectSyntax;
        if (username.len > COLUMN_EMAIL_SIZE) {
            return ZqlError.StatementParamTooLong;
        }
        // If there are more elements we can return syntax error
        if (tokens.next() != null) { return ZqlError.StatementIncorrectSyntax; }

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
        return ZqlError.StatementNotFound;
    }
}

fn executeInsert(allocator: *std.mem.Allocator, statement: *Statement, table: *Table) !void {
    if (table.numRows >= TABLE_MAX_ROWS) {
        return ZqlError.TableFull;
    }

    const freeRow: *Row = try rowSlot(allocator, table, table.numRows);
    saveRow(&statement.row, freeRow);
    table.numRows += 1;
}

fn printRow(stdout: std.fs.File.Writer, row: *Row) !void {
    try stdout.print("Row: {d}, {s}, {s}\n", .{ row.id, row.username, row.email });
}

fn executeSelect(allocator: *std.mem.Allocator, stdout: std.fs.File.Writer, statement: *Statement, table: *Table) !void {
    var row: Row = .{};
    var i: u32 = 0;
    while (i < table.numRows): (i += 1) {
        const currRow: *Row = try rowSlot(allocator, table, i);
        loadRow(currRow, &row);
        try printRow(stdout, &row);
    }
}

fn executeStatement(allocator: *std.mem.Allocator, stdout: std.fs.File.Writer, statement: *Statement, table: *Table) !void {
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

fn processLine(allocator: *std.mem.Allocator, stdin: anytype, stdout: std.fs.File.Writer, line: []u8, table: *Table) !void {

    if (line[0] == '.') {
        doMetaCommand(stdout, line) catch |err| {
            try stdout.print("Unrecognized command: {s}.\n", .{ line });
            return;
        };
    }
    else {
        var statement: Statement = .{};
        prepareStatement(line, &statement) catch |err| {
            switch(err) {
                ZqlError.StatementNotFound => {
                    try stdout.print("Unrecognized keyword at start of: {s}.\n", .{ line });
                },
                ZqlError.StatementIncorrectSyntax => {
                    try stdout.print("Incorrect syntax on statement: {s}.\n", .{ line });
                },
                ZqlError.StatementParamTooLong => {
                    try stdout.print("Parameter too long on statement: {s}.\n", .{ line });
                },
                else => unreachable,
            }
            return;
        };

        executeStatement(allocator, stdout, &statement, table) catch |err| {
            try stdout.print("Error executing command: {s}.\n", .{ line });
            return;
        };
        try stdout.print("Executed.\n", .{});
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

    var table: Table = (try newTable(pAllocator)).*;
    defer pAllocator.destroy(&table);

    while (true) {
        try printPrompt(stdout);

        var line: []u8 = readInput(pBufStream, pAllocator);
        defer pAllocator.free(line);

        try processLine(pAllocator, stdin, stdout, line, &table);
    }
}
