
const std = @import("std");

const ZqlError = error {
    StatementNotFound,
    StatementIncorrectSyntax,
    CommandError,
    TableFull,
    StatementParamTooLong,
    StatementErrorNegativeValue,
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

//const Table = struct {
//    numRows: u32,
//    pages: [TABLE_MAX_PAGES]?[]Row,
//};

const Table = struct {
    numRows: u32,
    pages: std.ArrayList(?std.ArrayList(?Row)),
    allocator: *std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: *std.mem.Allocator) !Self {
        var self = Self {
            .allocator = allocator,
            .numRows = 0,
            // Starting initCapacity to start an arraylist of nulls and initialising all with nulls
            .pages = try std.ArrayList(?std.ArrayList(?Row)).initCapacity(allocator, TABLE_MAX_PAGES),
        };
        // Initialise all the pages to TABLE_MAX_PAGES with null values so they can be accessed
        var i: usize = 0;
        while (i < 10) : (i += 1) {
            try self.pages.append(null);
        }
        return self;
    }

    pub fn deinit(self: Self) void {
        for (self.pages.items) |page, i| {
            // Only deinit pages that are not null
            if (page != null) {
                page.?.deinit();
            }
        }
        self.pages.deinit();
    }
};

const Statement = struct {
    type: StatementType = StatementType.SELECT,
    row: Row = .{},
};

// TODO: Make customizable
const MAX_INPUT_SIZE: usize = 1000;

fn saveRow(source: *Row, destination: *?Row) void {
    destination.* = source.*;
}

fn loadRow(source: *?Row, destination: *Row) void {
    destination.* = source.*.?;
}

fn rowSlot(allocator: *std.mem.Allocator, table: *Table, rowNum: u32) !*?Row {
    const pageNum: u32 = rowNum / ROWS_PER_PAGE;
    // TODO: Check max age
    if (table.pages.items[pageNum] == null) {
        table.pages.items[pageNum] = try std.ArrayList(?Row).initCapacity(allocator, ROWS_PER_PAGE);
        // Initialise all to null so they can be accessed as required
        var i: usize = 0;
        while (i < ROWS_PER_PAGE) : (i += 1) {
            try table.pages.items[pageNum].?.append(null);
        }
    }
    const rowOffset: u32 = rowNum % ROWS_PER_PAGE;
    return &(table.pages.items[pageNum].?.items[rowOffset]);
}

fn printPrompt(stdout: anytype) !void {
    try stdout.print("db > ", .{});
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
        const id = std.fmt.parseInt(i32, idRaw, 10) catch |err| return ZqlError.StatementIncorrectSyntax;
        if (id < 0) return ZqlError.StatementErrorNegativeValue;
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
        statement.row.id = @intCast(u32, id);
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

    var pFreeRow: *?Row = try rowSlot(allocator, table, table.numRows);
    saveRow(&statement.row, pFreeRow);
    table.numRows += 1;
}

fn printRow(stdout: anytype, row: *Row) !void {
    try stdout.print("Row: {d}, {s}, {s}\n", .{ row.id, row.username, row.email });
}

fn executeSelect(allocator: *std.mem.Allocator, stdout: anytype, statement: *Statement, table: *Table) !void {
    var row: Row = .{};
    var i: u32 = 0;
    while (i < table.numRows): (i += 1) {
        var pCurrRow: *?Row = try rowSlot(allocator, table, i);
        loadRow(pCurrRow, &row);
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

fn processLine(allocator: *std.mem.Allocator, stdout: anytype, line: []u8, table: *Table) !void {

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
                    try stdout.print("Incorrect statement: {s}.\n", .{ line });
                },
                ZqlError.StatementParamTooLong => {
                    try stdout.print("Parameter too long on statement: {s}.\n", .{ line });
                },
                ZqlError.StatementErrorNegativeValue => {
                    try stdout.print("Incorrect statement: ID must be positive.\n", .{});
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

    var table: *Table = &(try Table.init(pAllocator));
    defer table.deinit();

    while (true) {
        try printPrompt(stdout);

        var line: []u8 = readInput(pBufStream, pAllocator);
        defer pAllocator.free(line);

        try processLine(pAllocator, stdout, line, table);
    }
}


test "Test statement error" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [11]u8 = "Hello world".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Unrecognized keyword at start of: Hello world.\n"));
}

test "Test statement error" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [10]u8 = ".nocommand".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Unrecognized command: .nocommand.\n"));
}

test "Test statement select" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [6]u8 = "select".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Executing select.\nExecuted.\n"));
}

test "Test statement insert error no args" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [6]u8 = "insert".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Incorrect statement: insert.\n"));
}

test "Test statement insert error too many args" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [22]u8 = "insert 1 one two three".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Incorrect statement: insert 1 one two three.\n"));
}

test "Test statement insert error negative ID" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    var line: [23]u8 = "insert -1 one two three".*;
    try processLine(testAllocator, outList.writer(), &line, table);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, "Incorrect statement: ID must be positive.\n"));
}

test "Test statement insert and select" {
    const testAllocator = std.testing.allocator;
    var outList = std.ArrayList(u8).init(testAllocator);
    defer outList.deinit();

    var table: *Table = &(try Table.init(testAllocator));
    defer table.deinit();

    const testUsername: [4]u8 = "user".*;
    const testEmail: [17]u8 = "email@example.com".*;

    var lineInsert: []u8 = try std.fmt.allocPrint(
        testAllocator,
        "insert 1 {s} {s}",
        .{ &testUsername, &testEmail });
    defer testAllocator.free(lineInsert);
    try processLine(testAllocator, outList.writer(), lineInsert, table);
    try processLine(testAllocator, outList.writer(), lineInsert, table);
    var lineSelect: [6]u8 = "select".*;
    try processLine(testAllocator, outList.writer(), &lineSelect, table);
    const lineResultTemplate: *const [118]u8 =
        \\Executing insert.
        \\Executed.
        \\Executing insert.
        \\Executed.
        \\Executing select.
        \\Row: 1, {s}, {s}
        \\Row: 1, {s}, {s}
        \\Executed.
        \\
    ;
    var username: [COLUMN_USERNAME_SIZE]u8 = std.mem.zeroes([COLUMN_USERNAME_SIZE]u8);
    var email: [COLUMN_EMAIL_SIZE]u8 = std.mem.zeroes([COLUMN_EMAIL_SIZE]u8);
    std.mem.copy(u8, &username, &testUsername);
    std.mem.copy(u8, &email, &testEmail);
    var lineResult: []u8 = try std.fmt.allocPrint(
        testAllocator, lineResultTemplate, .{ username, email, username, email });
    defer testAllocator.free(lineResult);
    try std.testing.expect(
        std.mem.eql(u8, outList.items, lineResult));
}

// Currently needs to be extended to add the buffer
// 190 333 Parameter too long on statement: insert 1 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA email@example.com.
// Parameter too long on statement: insert 1 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA email@example.com.
// test "Test statement insert param too long"... FAIL (TestUnexpectedResult)
// test "Test statement insert param too long" {
//     const testAllocator = std.testing.allocator;
//     var outList = std.ArrayList(u8).init(testAllocator);
//     defer outList.deinit();
// 
//     var table: *Table = Table.init(testAllocator);
//     defer table.deinit();
// 
//     var testUsernameTooLong: [COLUMN_USERNAME_SIZE+1]u8 =
//         std.mem.zeroes([COLUMN_USERNAME_SIZE+1]u8);
//     std.mem.set(u8, &testUsernameTooLong, 'A');
// 
//     const testEmail: [17]u8 = "email@example.com".*;
// 
//     var lineInsert: []u8 = try std.fmt.allocPrint(
//         testAllocator,
//         "insert 1 {s} {s}",
//         .{ &testUsernameTooLong, &testEmail });
//     defer testAllocator.free(lineInsert);
//     try processLine(testAllocator, outList.writer(), lineInsert, table);
//     try processLine(testAllocator, outList.writer(), lineInsert, table);
//     const lineResultTemplate: *const [51]u8 =
//         "Parameter too long on statement: insert 1 {s} {s}.\n";
//     var username: [COLUMN_USERNAME_SIZE+1]u8 = std.mem.zeroes([COLUMN_USERNAME_SIZE+1]u8);
//     var email: [COLUMN_EMAIL_SIZE]u8 = std.mem.zeroes([COLUMN_EMAIL_SIZE]u8);
//     std.mem.copy(u8, &username, &testUsernameTooLong);
//     std.mem.copy(u8, &email, &testEmail);
//     var lineResult: []u8 = try std.fmt.allocPrint(
//         testAllocator, lineResultTemplate, .{ testUsernameTooLong, email });
//     defer testAllocator.free(lineResult);
//     std.debug.print("{d} {d} {s}", .{ outList.items.len, lineResult.len, outList.items });
//     try std.testing.expect(
//         std.mem.eql(u8, outList.items, lineResult));
// }
