"""A minimal FlatBuffers *writer*, enough for Arrow IPC metadata.

Only the write half is here, and that is a deliberate limit rather than an
unfinished one. Reading arbitrary FlatBuffers means honouring whatever layout
a producer chose — optional fields, vtable sharing, alignment it picked. But
*writing* means we choose the layout, so the encoder only has to be
self-consistent and spec-legal. That is a much smaller thing to get right, and
it is all Arrow IPC needs from us: we emit `Schema` and `RecordBatch` messages
and never parse one.

**Buffers are built back to front.** This is the part of FlatBuffers that
surprises people. `_head` starts at the end of the storage and walks
downwards, so the root table ends up nearest the front and every child it
points at is already written behind it. `offset()` is therefore "bytes written
so far", measured from the end, and every stored offset is a *backwards*
distance from the field to its target — which is why `_ref` subtracts.

A table is written as: field data, then a vtable listing where each field
landed, then a signed backwards offset from the table to that vtable. Absent
fields are simply not written, which is how a table stays small and how
readers of a newer schema tolerate an older writer.

No vtable deduplication. Two tables with identical shapes each get their own
vtable, costing a few bytes per message. Arrow messages are small and are
written once per batch, so the bytes are not worth the bookkeeping.
"""

from std.memory import memcpy


struct FlatBufferBuilder(Movable):
    """Builds one FlatBuffer, back to front.

    Not reusable: `finish` leaves the builder holding the completed buffer,
    and the intended lifecycle is one builder per message.
    """

    var _buf: List[UInt8]
    """Storage. Only `_buf[_head:]` is live; everything below `_head` is slack
    waiting to be written into."""

    var _head: Int
    """Index of the most recently written byte. Writing decrements it."""

    var _min_align: Int
    """The largest alignment any field has asked for. The finished buffer must
    be aligned to this for the root to be readable."""

    var _vtable: List[Int]
    """Per-slot offsets for the table being built, indexed by field slot.
    `0` means the field was not set and will be omitted."""

    var _table_start: Int
    """`offset()` at the point `start_table` was called."""

    var _in_table: Bool

    var _vec_elems: Int
    """Element count for the vector being built, for `end_vector` to write."""

    def __init__(out self, initial: Int = 1024):
        self._buf = List[UInt8]()
        self._buf.resize(initial, 0)
        self._head = initial
        self._min_align = 1
        self._vtable = List[Int]()
        self._table_start = 0
        self._in_table = False
        self._vec_elems = 0

    # ── storage ──────────────────────────────────────────────────────────

    def offset(self) -> Int:
        """Bytes written so far, which is also the position of the next write
        measured from the end of the buffer."""
        return len(self._buf) - self._head

    def _grow(mut self):
        """Double the storage, keeping the live bytes at the end.

        The live region has to stay flush with the end of the buffer because
        every offset already written is relative to that end.
        """
        var old_len = len(self._buf)
        var live = self.offset()
        var new_len = old_len * 2
        var bigger = List[UInt8]()
        bigger.resize(new_len, 0)
        if live > 0:
            memcpy(
                dest=bigger.unsafe_ptr() + (new_len - live),
                src=self._buf.unsafe_ptr() + self._head,
                count=live,
            )
        self._buf = bigger^
        self._head = new_len - live

    def _prep(mut self, size: Int, additional: Int):
        """Reserve room for `size` bytes plus `additional`, aligned to `size`.

        `additional` is what makes vectors work: a vector's elements must be
        aligned *after* the length prefix is written, so the caller asks for
        the padding that the prefix will consume.
        """
        if size > self._min_align:
            self._min_align = size
        var align_size = ((~(self.offset() + additional)) + 1) & (size - 1)
        while self._head < align_size + size + additional:
            self._grow()
        for _ in range(align_size):
            self._head -= 1
            self._buf[self._head] = 0

    def _place_bytes(mut self, value: UInt64, n: Int):
        """Write `n` little-endian bytes of `value`."""
        while self._head < n:
            self._grow()
        for i in range(n):
            self._head -= 1
            self._buf[self._head] = UInt8((value >> UInt64((n - 1 - i) * 8)) & 0xFF)

    # ── scalars ──────────────────────────────────────────────────────────

    def prepend_u8(mut self, v: UInt8):
        self._prep(1, 0)
        self._place_bytes(UInt64(v), 1)

    def prepend_bool(mut self, v: Bool):
        self.prepend_u8(UInt8(1) if v else UInt8(0))

    def prepend_i16(mut self, v: Int):
        self._prep(2, 0)
        self._place_bytes(UInt64(UInt16(v)), 2)

    def prepend_i32(mut self, v: Int):
        self._prep(4, 0)
        self._place_bytes(UInt64(UInt32(v)), 4)

    def prepend_i64(mut self, v: Int):
        self._prep(8, 0)
        self._place_bytes(UInt64(v), 8)

    def _ref(mut self, target: Int):
        """Write a uoffset pointing back to `target`.

        Offsets are relative and always backwards: the reader adds the stored
        value to the field's own position to reach the target, so the value is
        the distance from here to there.
        """
        self._prep(4, 0)
        var delta = self.offset() + 4 - target
        self._place_bytes(UInt64(UInt32(delta)), 4)

    # ── tables ───────────────────────────────────────────────────────────

    def start_table(mut self, num_slots: Int) raises:
        if self._in_table:
            raise Error("flatbuf: nested start_table")
        self._in_table = True
        self._vtable = List[Int]()
        self._vtable.resize(num_slots, 0)
        self._table_start = self.offset()

    def _slot(mut self, slot: Int):
        self._vtable[slot] = self.offset()

    def add_bool(mut self, slot: Int, v: Bool, default: Bool = False):
        """Add a field, unless it already holds the schema default.

        Omitting defaults is not just a size trick: a reader that asks for an
        absent field gets the default back, so writing it would be identical
        but larger.
        """
        if v == default:
            return
        self.prepend_bool(v)
        self._slot(slot)

    def add_i16(mut self, slot: Int, v: Int, default: Int = 0):
        if v == default:
            return
        self.prepend_i16(v)
        self._slot(slot)

    def add_i32(mut self, slot: Int, v: Int, default: Int = 0):
        if v == default:
            return
        self.prepend_i32(v)
        self._slot(slot)

    def add_i64(mut self, slot: Int, v: Int, default: Int = 0):
        if v == default:
            return
        self.prepend_i64(v)
        self._slot(slot)

    def add_u8(mut self, slot: Int, v: UInt8, default: UInt8 = 0):
        if v == default:
            return
        self.prepend_u8(v)
        self._slot(slot)

    def add_offset(mut self, slot: Int, target: Int):
        """Add a reference field (table, string or vector). `0` means absent."""
        if target == 0:
            return
        self._ref(target)
        self._slot(slot)

    def end_table(mut self) raises -> Int:
        """Finish the table and return its offset.

        Writes the vtable, then the table's own backwards pointer to it. The
        vtable is trimmed to the last field actually set, which is what lets a
        reader of a longer schema see the extra fields as absent rather than
        as garbage.
        """
        if not self._in_table:
            raise Error("flatbuf: end_table without start_table")

        # The soffset to the vtable occupies the first 4 bytes of the table.
        self.prepend_i32(0)
        var table_end = self.offset()

        var last_set = -1
        for i in range(len(self._vtable)):
            if self._vtable[i] != 0:
                last_set = i
        var slots = last_set + 1

        # vtable: [vtable_size:i16][table_size:i16][slot offsets:i16...],
        # each slot offset measured from the start of the table.
        for i in range(slots - 1, -1, -1):
            var off = self._vtable[i]
            self.prepend_i16(0 if off == 0 else (table_end - off))
        self.prepend_i16(table_end - self._table_start)
        self.prepend_i16((slots + 2) * 2)

        var vtable_pos = self.offset()

        # Patch the table's soffset in place: positive means the vtable sits
        # behind the table, which it always does here.
        var delta = vtable_pos - table_end
        var at = len(self._buf) - table_end
        for i in range(4):
            self._buf[at + i] = UInt8((UInt32(delta) >> UInt32(i * 8)) & 0xFF)

        self._in_table = False
        return table_end

    # ── strings and vectors ──────────────────────────────────────────────

    def create_string(mut self, s: String) raises -> Int:
        """Write a length-prefixed, NUL-terminated UTF-8 string."""
        if self._in_table:
            raise Error("flatbuf: create_string inside a table")
        var b = s.as_bytes()
        var n = len(b)
        self._prep(1, 1)
        self._head -= 1
        self._buf[self._head] = 0  # the NUL readers expect
        self._prep(4, n)
        while self._head < n:
            self._grow()
        for i in range(n - 1, -1, -1):
            self._head -= 1
            self._buf[self._head] = b[i]
        self.prepend_i32(n)
        return self.offset()

    def start_vector(mut self, elem_size: Int, count: Int, align: Int) raises:
        """Begin a vector; elements are prepended in reverse order."""
        if self._in_table:
            raise Error("flatbuf: start_vector inside a table")
        self._vec_elems = count
        self._prep(4, elem_size * count)
        if align > 4:
            self._prep(align, elem_size * count)

    def end_vector(mut self) -> Int:
        self.prepend_i32(self._vec_elems)
        return self.offset()

    def create_offset_vector(mut self, offsets: List[Int]) raises -> Int:
        """A vector of references, written back to front."""
        self.start_vector(4, len(offsets), 4)
        for i in range(len(offsets) - 1, -1, -1):
            self._ref(offsets[i])
        return self.end_vector()

    # ── finishing ────────────────────────────────────────────────────────

    def finish(mut self, root: Int) raises -> List[UInt8]:
        """Write the root offset and return the finished buffer."""
        if self._in_table:
            raise Error("flatbuf: finish with a table still open")
        self._prep(self._min_align, 4)
        self._ref(root)
        var out = List[UInt8]()
        var n = self.offset()
        out.resize(n, 0)
        memcpy(
            dest=out.unsafe_ptr(),
            src=self._buf.unsafe_ptr() + self._head,
            count=n,
        )
        return out^
