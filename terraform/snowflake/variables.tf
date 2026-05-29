variable "snowflake_org" {
    description = "Snowflake organization name"
    type = string 
}

variable "snowflake_account"{
    description = "Snowflake account name"
    type = string 
}

variable "snowflake_user"{
    description = "Snowflake username"
    type = string 
}

variable "snowflake_password"{
    description = "Snowflake password"
    type = string
    sensitive = true
}