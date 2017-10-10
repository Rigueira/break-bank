extern crate rand;
use std::env;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
const OUTPUT_FOLDER_NAME:&'static str="Output";
const QUERY_FILE_NAME:&'static str="Queries.sql";
const LOG_FILE_NAME:&'static str="Log.txt";
const DATABASE_NAME:&'static str="sTestes";
const IMPORT_ID:&'static str="sicred jul-set 10-10-2017 16:46 ";
fn main() {
    // Global execution variables.
    let mut valid_file_count=0;
    let mut error_file_count=0;
    let mut insert_commands_generated=0;

    let args:Vec<String>=env::args().collect();
    let mut path=set_output_folder(&args);
    path.push(QUERY_FILE_NAME);
    let mut query_file=File::create(&path).unwrap();
    path.pop();
    path.push(LOG_FILE_NAME);
    let mut log_file=File::create(&path).unwrap();
    path.pop();
    path.pop();
    let random_import_id=rand::random::<u32>();
    log_file.write_all(format!("Processing Directory: {}\n\n",&path.display()).as_bytes()).unwrap();
    for entry in fs::read_dir(&path).unwrap() { // Iterate over the directory.
        if let Ok(entry)=entry { // Validate directory entry (file or folder).
            if let Ok(meta)=entry.metadata() { // Read entry (file or folder) metadata.
                if meta.is_dir() {
                    log_file.write_all(format!("Skipping Directory: {}\n\n",entry.path().display()).as_bytes()).unwrap();
                    continue;
                } else {
                    let file=File::open(entry.path()).unwrap();
                    let reader=BufReader::new(file);
                    let mut line_index=0;
                    let mut header_line_count=0;
                    let mut record_line_count=0;
                    let mut trailer_line_count=0;
                    let mut bank_code=String::new();
                    let mut nsa_code=String::new();
                    valid_file_count+=1;
                    for line in reader.lines() {
                        line_index+=1;
                        let line_string=match line {
                            Ok(text)=>text.to_string(),
                            Err(_)=>"ERROR READING LINE".to_string(),
                        };
                        if line_string.len()<150 { //Invalid file.
                            valid_file_count-=1;
                            error_file_count+=1;
                            log_file.write_all(format!("Invalid File: {}\nLine Number {} Contains Just {} Chars\n\n",entry.path().display(),line_index,line_string.len()).as_bytes()).unwrap();
                            break;
                        }
                        match line_string[..1].as_ref() {
                            "A"=>{ // Header.
                                header_line_count+=1;
                                bank_code.clear();
                                bank_code.push_str(&line_string[42..45]);
                                nsa_code.clear();
                                nsa_code.push_str(&line_string[73..79]);
                                if header_line_count>1 {
                                    log_file.write_all(format!("!!! Multiple Header: bank {} date {} NSA {}\n",&bank_code,&line_string[65..73],&nsa_code).as_bytes()).unwrap();
                                } else {
                                    log_file.write_all(format!("Header: bank {} date {} NSA {}\n",&bank_code,&line_string[65..73],&nsa_code).as_bytes()).unwrap();
                                }
                            },
                            "G"=>{ // Records.
                                if insert_commands_generated==0 {
                                    query_file.write_all(format!("use {};\n",DATABASE_NAME).as_bytes()).unwrap();
                                }
                                record_line_count+=1;
                                insert_commands_generated+=1;
                                let mut query_string=String::new();
                                query_string.push_str("insert into dbo.retorno_bancario (banco,import_id,g_line,conta,nossonumero,pagamento,credito,valor,file_code,file_index) values (");
                                query_string.push_str(&bank_code); // banco
                                query_string.push_str(",\'");
                                query_string.push_str(IMPORT_ID); // import_id
                                query_string.push_str(&random_import_id.to_string()[..]);
                                query_string.push_str("\',\'");
                                query_string.push_str(&line_string); // g_line
                                query_string.push_str("\',\'");
                                query_string.push_str(&line_string[1..21]); // conta
                                query_string.push_str("\',");
                                query_string.push_str(&line_string[64..79]); // nossonumero
                                query_string.push_str(",");
                                query_string.push_str(&line_string[21..29]); // pagamento
                                query_string.push_str(",");
                                query_string.push_str(&line_string[29..37]); //credito
                                query_string.push_str(",");
                                query_string.push_str(&line_string[81..91]); // valor (integral)
                                query_string.push_str(".");
                                query_string.push_str(&line_string[91..93]); // valor (decimal)
                                query_string.push_str(",");
                                query_string.push_str(&nsa_code); // file_code
                                query_string.push_str(",");
                                query_string.push_str(&line_string[100..108]); // file_index
                                query_string.push_str(");\n");
                                query_file.write_all(query_string.as_bytes()).unwrap();
                            },
                            "Z"=>{ // Trailer/footer.
                                trailer_line_count+=1;
                                if trailer_line_count>1 {
                                    log_file.write_all(format!("!!! Multiple Trailer\n").as_bytes()).unwrap();
                                }
                            },
                            _=>{ // Any other kind of line.
                                log_file.write_all(format!("!!! Unknown Line Found: {}\n",line_index).as_bytes()).unwrap();
                            },
                        }
                    }
                    // End of file.
                    log_file.write_all(format!("Total Lines: {}\nHeader Lines: {}\nRecord Lines: {}\nTrailer Lines: {}\nProcessing Finished\n\n",line_index,header_line_count,record_line_count,trailer_line_count).as_bytes()).unwrap();
                }
            }
        }
    }
    // End of directory list.
    log_file.write_all(format!("Valid Files: {}\nInvalid Files: {}\nInsert Lines Creted: {}\n",valid_file_count,error_file_count,insert_commands_generated).as_bytes()).unwrap();
}

// Validates command line input path and returns the output folder.
fn set_output_folder(args:&Vec<String>) -> PathBuf {
    let mut path=env::current_dir().unwrap(); // Set current directory as working directory.
    if args.len()>=2 {
        let new_path=PathBuf::from(&args[1][..]);
        if new_path.exists() && new_path.is_dir() && new_path.is_absolute() {
            path.push(&new_path); // Replace the working directory with the supplied directory.
        }
    }
    path.push(OUTPUT_FOLDER_NAME);
    if !path.exists() { // A FILE with the same name as the OUTPUT_FOLDER_NAME will cause this test to fail.
        fs::create_dir(&path).unwrap();
    }
    path
}
