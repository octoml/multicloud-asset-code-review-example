import s3_utils

def test_get_byte_offsets_and_parts_large():
    obj_size = 17179869184
    part_size = 1024 * 1024 * 50
    task_count = 10

    (byte_start_positions, byte_end_positions, starting_part_nums) = (
        s3_utils._get_byte_offsets_and_parts(obj_size, task_count, part_size)
    )

    assert len(byte_start_positions) == task_count
    assert len(starting_part_nums) == task_count

    assert byte_start_positions[0] == 0
    assert byte_end_positions[-1] == obj_size - 1

    print(byte_end_positions)
    print(starting_part_nums)

    total = 0

    for i, (byte_start, byte_end) in enumerate(
        zip(byte_start_positions, byte_end_positions, strict=False)
    ):
        assert byte_start < byte_end
        assert byte_end <= (obj_size - 1)
        sz = byte_end - byte_start + 1

        # There should be no gaps between tasks
        if i > task_count - 1:
            assert byte_end == byte_start_positions[i + 1] - 1

        print(sz)
        print(sz % part_size)

        # last task may have have a remainder but all others should be exact multiples
        # of part_size
        assert i == (task_count - 1) or sz % part_size == 0

        total += byte_end - byte_start + 1

        # Check starting_part_nums reflect the byte offsets
        if i < task_count - 1:
            part_count = starting_part_nums[i + 1] - starting_part_nums[i]
            assert part_count == sz // part_size

    # Total size of all tasks should equal the object size
    assert total == obj_size

    # First part is always 1 since it's 1-indexed
    assert starting_part_nums[0] == 1