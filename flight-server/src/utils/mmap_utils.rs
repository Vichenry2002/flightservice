use std::{env, fs::OpenOptions, io::Cursor};
use arrow::{record_batch::RecordBatch, ipc::{writer::FileWriter, reader::FileReader}};
use arrow_flight::Ticket;
use memmap2::{MmapMut, Mmap};
use tonic::Status;
use uuid::Uuid;

//TODO: IMPLEMENT AND TEST
/// Serialize a RecordBatch to a memory-mapped file and return a Ticket with the file path.
pub fn batch_to_mmap(batch: &RecordBatch) -> Result<Ticket, Status> {
    let unique_id = Uuid::new_v4().to_string();
    let path = env::var("DATA_PATH").unwrap_or_else(|_| "./data".to_string());
    let file_path = format!("{}/batch_{}.mmap",path,unique_id);

    match write_record_batch_to_file(batch, &file_path) {
        Ok(_) => Ok(Ticket::new(file_path)),
        Err(_) => Err(Status::internal("Failed to write RecordBatch to memory-mapped file"))
    }
}

//TODO: IMPLEMENT AND TEST
/// Helper function to write a RecordBatch to a file using memory mapping.
/// Helper function to write a RecordBatch to a file using memory mapping.
fn write_record_batch_to_file(batch: &RecordBatch, file_path: &str) -> Result<(), Status> {
    // Create or open the file
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)
        .map_err(|_| Status::internal("Failed to create or open file"))?;

    // Must create a buffer because MmapMut type does not implement the std::io::Write trait, which is required by FileWriter.
    let mut buffer = Vec::new();

    // Create a FileWriter to serialize the RecordBatch to the buffer
    {
        let mut writer = FileWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|_| Status::internal("Failed to create FileWriter"))?;
        writer.write(batch).map_err(|_| Status::internal("Failed to write RecordBatch to buffer"))?;
        writer.finish().map_err(|_| Status::internal("Failed to finish writing RecordBatch"))?;
    }

    // Set the file size
    file.set_len(buffer.len() as u64)
        .map_err(|_| Status::internal("Failed to set file length"))?;

    // Memory map the file
    let mut mmap = unsafe {
        MmapMut::map_mut(&file).map_err(|_| Status::internal("Failed to memory map the file"))?
    };

    // Copy the buffer to the memory-mapped file
    mmap[..buffer.len()].copy_from_slice(&buffer);

    // Flush the memory-mapped file to disk
    mmap.flush().map_err(|_| Status::internal("Failed to flush memory-mapped file"))?;

    Ok(())
}

//TODO: IMPLEMENT AND TEST
// Load a RecordBatch from a memory-mapped file.
pub fn mmap_to_batch(ticket: &Ticket) -> Result<RecordBatch, Status> {

    let file_path = std::str::from_utf8(&ticket.ticket)
        .map_err(|_| Status::internal("Failed to convert ticket to UTF-8 string"))?
        .to_string();
    // Open the memory-mapped file
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)
        .map_err(|_| Status::internal("Failed to open memory-mapped file"))?;

    // Memory map the file
    let mmap = unsafe {
        Mmap::map(&file).map_err(|_| Status::internal("Failed to memory map the file"))?
    };

    // Create a cursor for the memory-mapped region
    let cursor = Cursor::new(&mmap[..]);

    // Create a FileReader to deserialize the RecordBatch from the cursor
    let mut reader = FileReader::try_new(cursor, None)
        .map_err(|_| Status::internal("Failed to create FileReader"))?;

    // Read the first RecordBatch from the FileReader
    reader
        .next()
        .ok_or_else(|| Status::internal("No RecordBatch found in file"))?
        .map_err(|_| Status::internal("Failed to read RecordBatch from file"))
}
