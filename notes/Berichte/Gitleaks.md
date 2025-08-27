Collect File Endings, Size, Directory Depth, File Count, Hash Files to find out, how many are unique, Fileendings of secrets

{
    File Count
    Directory Depth
    Files: {
        Ending
        Size
        Secret / nil
    }
}

RuleID: generic-api-key,string
Description: Detected a Generic API Key, potentially exposing access to various services and sensitive operations.,string
StartLine: 1745,int
EndLine: 1745,int
StartColumn: 4,int
EndColumn: 43,int
Line: 
D:openssh-keygen so:libc.musl-x86_64.so.1 so:libcrypto.so.41 so:libz.so.1,string
Match: openssh-keygen so:libc.musl-x86_64.so.1 ,string
Secret: libc.musl-x86_64.so.1,string
File: ../Docker-Images/sha256:8d8d56807eb95e9b01ef9f66d7939ce4e117d5432ef0eccf533ea4c0ed971db3/lib/apk/db/installed,string
SymlinkFile: ,string
Commit: ,string
Link: ,string
Entropy: 3.88018,float32
Author: ,string
Email: ,string
Date: ,string
Message: ,string
Tags: [],[]string
Fingerprint: ../Docker-Images/sha256:8d8d56807eb95e9b01ef9f66d7939ce4e117d5432ef0eccf533ea4c0ed971db3/lib/apk/db/installed:generic-api-key:1745,string