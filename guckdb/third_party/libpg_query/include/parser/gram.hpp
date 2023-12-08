/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     INTEGER_DIVISION = 270,
     POWER_OF = 271,
     LAMBDA_ARROW = 272,
     DOUBLE_ARROW = 273,
     LESS_EQUALS = 274,
     GREATER_EQUALS = 275,
     NOT_EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASOF = 297,
     ASSERTION = 298,
     ASSIGNMENT = 299,
     ASYMMETRIC = 300,
     AT = 301,
     ATTACH = 302,
     ATTRIBUTE = 303,
     AUTHORIZATION = 304,
     BACKWARD = 305,
     BEFORE = 306,
     BEGIN_P = 307,
     BETWEEN = 308,
     BIGINT = 309,
     BINARY = 310,
     BIT = 311,
     BOOLEAN_P = 312,
     BOTH = 313,
     BY = 314,
     CACHE = 315,
     CALL_P = 316,
     CALLED = 317,
     CASCADE = 318,
     CASCADED = 319,
     CASE = 320,
     CAST = 321,
     CATALOG_P = 322,
     CHAIN = 323,
     CHAR_P = 324,
     CHARACTER = 325,
     CHARACTERISTICS = 326,
     CHECK_P = 327,
     CHECKPOINT = 328,
     CLASS = 329,
     CLOSE = 330,
     CLUSTER = 331,
     COALESCE = 332,
     COLLATE = 333,
     COLLATION = 334,
     COLUMN = 335,
     COLUMNS = 336,
     COMMENT = 337,
     COMMENTS = 338,
     COMMIT = 339,
     COMMITTED = 340,
     COMPRESSION = 341,
     CONCURRENTLY = 342,
     CONFIGURATION = 343,
     CONFLICT = 344,
     CONNECTION = 345,
     CONSTRAINT = 346,
     CONSTRAINTS = 347,
     CONTENT_P = 348,
     CONTINUE_P = 349,
     CONVERSION_P = 350,
     COPY = 351,
     COST = 352,
     CREATE_P = 353,
     CROSS = 354,
     CSV = 355,
     CUBE = 356,
     CURRENT_P = 357,
     CURSOR = 358,
     CYCLE = 359,
     DATA_P = 360,
     DATABASE = 361,
     DAY_P = 362,
     DAYS_P = 363,
     DEALLOCATE = 364,
     DEC = 365,
     DECIMAL_P = 366,
     DECLARE = 367,
     DEFAULT = 368,
     DEFAULTS = 369,
     DEFERRABLE = 370,
     DEFERRED = 371,
     DEFINER = 372,
     DELETE_P = 373,
     DELIMITER = 374,
     DELIMITERS = 375,
     DEPENDS = 376,
     DESC_P = 377,
     DESCRIBE = 378,
     DETACH = 379,
     DICTIONARY = 380,
     DIRECTED = 381,
     DISABLE_P = 382,
     DISCARD = 383,
     DISTINCT = 384,
     DO = 385,
     DOCUMENT_P = 386,
     DOMAIN_P = 387,
     DOUBLE_P = 388,
     DROP = 389,
     EACH = 390,
     ELSE = 391,
     ENABLE_P = 392,
     ENCODING = 393,
     ENCRYPTED = 394,
     END_P = 395,
     ENUM_P = 396,
     ESCAPE = 397,
     EVENT = 398,
     EXCEPT = 399,
     EXCLUDE = 400,
     EXCLUDING = 401,
     EXCLUSIVE = 402,
     EXECUTE = 403,
     EXISTS = 404,
     EXPLAIN = 405,
     EXPORT_P = 406,
     EXPORT_STATE = 407,
     EXTENSION = 408,
     EXTERNAL = 409,
     EXTRACT = 410,
     FALSE_P = 411,
     FAMILY = 412,
     FETCH = 413,
     FILTER = 414,
     FIRST_P = 415,
     FLOAT_P = 416,
     FOLLOWING = 417,
     FOR = 418,
     FORCE = 419,
     FOREIGN = 420,
     FORWARD = 421,
     FREEZE = 422,
     FROM = 423,
     FULL = 424,
     FUNCTION = 425,
     FUNCTIONS = 426,
     GENERATED = 427,
     GLOB = 428,
     GLOBAL = 429,
     GRANT = 430,
     GRANTED = 431,
     GROUP_P = 432,
     GROUPING = 433,
     GROUPING_ID = 434,
     HANDLER = 435,
     HAVING = 436,
     HEADER_P = 437,
     HOLD = 438,
     HOUR_P = 439,
     HOURS_P = 440,
     IDENTITY_P = 441,
     IF_P = 442,
     IGNORE_P = 443,
     ILIKE = 444,
     IMMEDIATE = 445,
     IMMUTABLE = 446,
     IMPLICIT_P = 447,
     IMPORT_P = 448,
     IN_P = 449,
     INCLUDE_P = 450,
     INCLUDING = 451,
     INCREMENT = 452,
     INDEX = 453,
     INDEXES = 454,
     INHERIT = 455,
     INHERITS = 456,
     INITIALLY = 457,
     INLINE_P = 458,
     INNER_P = 459,
     INOUT = 460,
     INPUT_P = 461,
     INSENSITIVE = 462,
     INSERT = 463,
     INSTALL = 464,
     INSTEAD = 465,
     INT_P = 466,
     INTEGER = 467,
     INTERSECT = 468,
     INTERVAL = 469,
     INTO = 470,
     INVOKER = 471,
     IS = 472,
     ISNULL = 473,
     ISOLATION = 474,
     JOIN = 475,
     JSON = 476,
     KEY = 477,
     LABEL = 478,
     LANGUAGE = 479,
     LARGE_P = 480,
     LAST_P = 481,
     LATERAL_P = 482,
     LEADING = 483,
     LEAKPROOF = 484,
     LEFT = 485,
     LEVEL = 486,
     LIKE = 487,
     LIMIT = 488,
     LISTEN = 489,
     LOAD = 490,
     LOCAL = 491,
     LOCATION = 492,
     LOCK_P = 493,
     LOCKED = 494,
     LOGGED = 495,
     MACRO = 496,
     MAP = 497,
     MAPPING = 498,
     MATCH = 499,
     MATERIALIZED = 500,
     MAXVALUE = 501,
     METHOD = 502,
     MICROSECOND_P = 503,
     MICROSECONDS_P = 504,
     MILLISECOND_P = 505,
     MILLISECONDS_P = 506,
     MINUTE_P = 507,
     MINUTES_P = 508,
     MINVALUE = 509,
     MODE = 510,
     MONTH_P = 511,
     MONTHS_P = 512,
     MOVE = 513,
     NAME_P = 514,
     NAMES = 515,
     NATIONAL = 516,
     NATURAL = 517,
     NCHAR = 518,
     NEW = 519,
     NEXT = 520,
     NO = 521,
     NONE = 522,
     NOT = 523,
     NOTHING = 524,
     NOTIFY = 525,
     NOTNULL = 526,
     NOWAIT = 527,
     NULL_P = 528,
     NULLIF = 529,
     NULLS_P = 530,
     NUMERIC = 531,
     OBJECT_P = 532,
     OF = 533,
     OFF = 534,
     OFFSET = 535,
     OIDS = 536,
     OLD = 537,
     ON = 538,
     ONLY = 539,
     OPERATOR = 540,
     OPTION = 541,
     OPTIONS = 542,
     OR = 543,
     ORDER = 544,
     ORDINALITY = 545,
     OUT_P = 546,
     OUTER_P = 547,
     OVER = 548,
     OVERLAPS = 549,
     OVERLAY = 550,
     OVERRIDING = 551,
     OWNED = 552,
     OWNER = 553,
     PARALLEL = 554,
     PARSER = 555,
     PARTIAL = 556,
     PARTITION = 557,
     PASSING = 558,
     PASSWORD = 559,
     PERCENT = 560,
     PIVOT = 561,
     PIVOT_LONGER = 562,
     PIVOT_WIDER = 563,
     PKFK = 564,
     PLACING = 565,
     PLANS = 566,
     POLICY = 567,
     POSITION = 568,
     POSITIONAL = 569,
     PRAGMA_P = 570,
     PRECEDING = 571,
     PRECISION = 572,
     PREPARE = 573,
     PREPARED = 574,
     PRESERVE = 575,
     PRIMARY = 576,
     PRIOR = 577,
     PRIVILEGES = 578,
     PROCEDURAL = 579,
     PROCEDURE = 580,
     PROGRAM = 581,
     PUBLICATION = 582,
     QUALIFY = 583,
     QUOTE = 584,
     RAI = 585,
     RANGE = 586,
     READ_P = 587,
     REAL = 588,
     REASSIGN = 589,
     RECHECK = 590,
     RECURSIVE = 591,
     REF = 592,
     REFERENCES = 593,
     REFERENCING = 594,
     REFRESH = 595,
     REINDEX = 596,
     RELATIVE_P = 597,
     RELEASE = 598,
     RENAME = 599,
     REPEATABLE = 600,
     REPLACE = 601,
     REPLICA = 602,
     RESET = 603,
     RESPECT_P = 604,
     RESTART = 605,
     RESTRICT = 606,
     RETURNING = 607,
     RETURNS = 608,
     REVOKE = 609,
     RIGHT = 610,
     ROLE = 611,
     ROLLBACK = 612,
     ROLLUP = 613,
     ROW = 614,
     ROWS = 615,
     RULE = 616,
     SAMPLE = 617,
     SAVEPOINT = 618,
     SCHEMA = 619,
     SCHEMAS = 620,
     SCROLL = 621,
     SEARCH = 622,
     SECOND_P = 623,
     SECONDS_P = 624,
     SECURITY = 625,
     SELECT = 626,
     SELF = 627,
     SEMI = 628,
     SEQUENCE = 629,
     SEQUENCES = 630,
     SERIALIZABLE = 631,
     SERVER = 632,
     SESSION = 633,
     SET = 634,
     SETOF = 635,
     SETS = 636,
     SHARE = 637,
     SHOW = 638,
     SIMILAR = 639,
     SIMPLE = 640,
     SKIP = 641,
     SMALLINT = 642,
     SNAPSHOT = 643,
     SOME = 644,
     SQL_P = 645,
     STABLE = 646,
     STANDALONE_P = 647,
     START = 648,
     STATEMENT = 649,
     STATISTICS = 650,
     STDIN = 651,
     STDOUT = 652,
     STORAGE = 653,
     STORED = 654,
     STRICT_P = 655,
     STRIP_P = 656,
     STRUCT = 657,
     SUBSCRIPTION = 658,
     SUBSTRING = 659,
     SUMMARIZE = 660,
     SYMMETRIC = 661,
     SYSID = 662,
     SYSTEM_P = 663,
     TABLE = 664,
     TABLES = 665,
     TABLESAMPLE = 666,
     TABLESPACE = 667,
     TEMP = 668,
     TEMPLATE = 669,
     TEMPORARY = 670,
     TEXT_P = 671,
     THEN = 672,
     TIME = 673,
     TIMESTAMP = 674,
     TO = 675,
     TRAILING = 676,
     TRANSACTION = 677,
     TRANSFORM = 678,
     TREAT = 679,
     TRIGGER = 680,
     TRIM = 681,
     TRUE_P = 682,
     TRUNCATE = 683,
     TRUSTED = 684,
     TRY_CAST = 685,
     TYPE_P = 686,
     TYPES_P = 687,
     UNBOUNDED = 688,
     UNCOMMITTED = 689,
     UNDIRECTED = 690,
     UNENCRYPTED = 691,
     UNION = 692,
     UNIQUE = 693,
     UNKNOWN = 694,
     UNLISTEN = 695,
     UNLOGGED = 696,
     UNPIVOT = 697,
     UNTIL = 698,
     UPDATE = 699,
     USE_P = 700,
     USER = 701,
     USING = 702,
     VACUUM = 703,
     VALID = 704,
     VALIDATE = 705,
     VALIDATOR = 706,
     VALUE_P = 707,
     VALUES = 708,
     VARCHAR = 709,
     VARIADIC = 710,
     VARYING = 711,
     VERBOSE = 712,
     VERSION_P = 713,
     VIEW = 714,
     VIEWS = 715,
     VIRTUAL = 716,
     VOLATILE = 717,
     WHEN = 718,
     WHERE = 719,
     WHITESPACE_P = 720,
     WINDOW = 721,
     WITH = 722,
     WITHIN = 723,
     WITHOUT = 724,
     WORK = 725,
     WRAPPER = 726,
     WRITE_P = 727,
     XML_P = 728,
     XMLATTRIBUTES = 729,
     XMLCONCAT = 730,
     XMLELEMENT = 731,
     XMLEXISTS = 732,
     XMLFOREST = 733,
     XMLNAMESPACES = 734,
     XMLPARSE = 735,
     XMLPI = 736,
     XMLROOT = 737,
     XMLSERIALIZE = 738,
     XMLTABLE = 739,
     YEAR_P = 740,
     YEARS_P = 741,
     YES_P = 742,
     ZONE = 743,
     NOT_LA = 744,
     NULLS_LA = 745,
     WITH_LA = 746,
     POSTFIXOP = 747,
     UMINUS = 748
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define INTEGER_DIVISION 270
#define POWER_OF 271
#define LAMBDA_ARROW 272
#define DOUBLE_ARROW 273
#define LESS_EQUALS 274
#define GREATER_EQUALS 275
#define NOT_EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASOF 297
#define ASSERTION 298
#define ASSIGNMENT 299
#define ASYMMETRIC 300
#define AT 301
#define ATTACH 302
#define ATTRIBUTE 303
#define AUTHORIZATION 304
#define BACKWARD 305
#define BEFORE 306
#define BEGIN_P 307
#define BETWEEN 308
#define BIGINT 309
#define BINARY 310
#define BIT 311
#define BOOLEAN_P 312
#define BOTH 313
#define BY 314
#define CACHE 315
#define CALL_P 316
#define CALLED 317
#define CASCADE 318
#define CASCADED 319
#define CASE 320
#define CAST 321
#define CATALOG_P 322
#define CHAIN 323
#define CHAR_P 324
#define CHARACTER 325
#define CHARACTERISTICS 326
#define CHECK_P 327
#define CHECKPOINT 328
#define CLASS 329
#define CLOSE 330
#define CLUSTER 331
#define COALESCE 332
#define COLLATE 333
#define COLLATION 334
#define COLUMN 335
#define COLUMNS 336
#define COMMENT 337
#define COMMENTS 338
#define COMMIT 339
#define COMMITTED 340
#define COMPRESSION 341
#define CONCURRENTLY 342
#define CONFIGURATION 343
#define CONFLICT 344
#define CONNECTION 345
#define CONSTRAINT 346
#define CONSTRAINTS 347
#define CONTENT_P 348
#define CONTINUE_P 349
#define CONVERSION_P 350
#define COPY 351
#define COST 352
#define CREATE_P 353
#define CROSS 354
#define CSV 355
#define CUBE 356
#define CURRENT_P 357
#define CURSOR 358
#define CYCLE 359
#define DATA_P 360
#define DATABASE 361
#define DAY_P 362
#define DAYS_P 363
#define DEALLOCATE 364
#define DEC 365
#define DECIMAL_P 366
#define DECLARE 367
#define DEFAULT 368
#define DEFAULTS 369
#define DEFERRABLE 370
#define DEFERRED 371
#define DEFINER 372
#define DELETE_P 373
#define DELIMITER 374
#define DELIMITERS 375
#define DEPENDS 376
#define DESC_P 377
#define DESCRIBE 378
#define DETACH 379
#define DICTIONARY 380
#define DIRECTED 381
#define DISABLE_P 382
#define DISCARD 383
#define DISTINCT 384
#define DO 385
#define DOCUMENT_P 386
#define DOMAIN_P 387
#define DOUBLE_P 388
#define DROP 389
#define EACH 390
#define ELSE 391
#define ENABLE_P 392
#define ENCODING 393
#define ENCRYPTED 394
#define END_P 395
#define ENUM_P 396
#define ESCAPE 397
#define EVENT 398
#define EXCEPT 399
#define EXCLUDE 400
#define EXCLUDING 401
#define EXCLUSIVE 402
#define EXECUTE 403
#define EXISTS 404
#define EXPLAIN 405
#define EXPORT_P 406
#define EXPORT_STATE 407
#define EXTENSION 408
#define EXTERNAL 409
#define EXTRACT 410
#define FALSE_P 411
#define FAMILY 412
#define FETCH 413
#define FILTER 414
#define FIRST_P 415
#define FLOAT_P 416
#define FOLLOWING 417
#define FOR 418
#define FORCE 419
#define FOREIGN 420
#define FORWARD 421
#define FREEZE 422
#define FROM 423
#define FULL 424
#define FUNCTION 425
#define FUNCTIONS 426
#define GENERATED 427
#define GLOB 428
#define GLOBAL 429
#define GRANT 430
#define GRANTED 431
#define GROUP_P 432
#define GROUPING 433
#define GROUPING_ID 434
#define HANDLER 435
#define HAVING 436
#define HEADER_P 437
#define HOLD 438
#define HOUR_P 439
#define HOURS_P 440
#define IDENTITY_P 441
#define IF_P 442
#define IGNORE_P 443
#define ILIKE 444
#define IMMEDIATE 445
#define IMMUTABLE 446
#define IMPLICIT_P 447
#define IMPORT_P 448
#define IN_P 449
#define INCLUDE_P 450
#define INCLUDING 451
#define INCREMENT 452
#define INDEX 453
#define INDEXES 454
#define INHERIT 455
#define INHERITS 456
#define INITIALLY 457
#define INLINE_P 458
#define INNER_P 459
#define INOUT 460
#define INPUT_P 461
#define INSENSITIVE 462
#define INSERT 463
#define INSTALL 464
#define INSTEAD 465
#define INT_P 466
#define INTEGER 467
#define INTERSECT 468
#define INTERVAL 469
#define INTO 470
#define INVOKER 471
#define IS 472
#define ISNULL 473
#define ISOLATION 474
#define JOIN 475
#define JSON 476
#define KEY 477
#define LABEL 478
#define LANGUAGE 479
#define LARGE_P 480
#define LAST_P 481
#define LATERAL_P 482
#define LEADING 483
#define LEAKPROOF 484
#define LEFT 485
#define LEVEL 486
#define LIKE 487
#define LIMIT 488
#define LISTEN 489
#define LOAD 490
#define LOCAL 491
#define LOCATION 492
#define LOCK_P 493
#define LOCKED 494
#define LOGGED 495
#define MACRO 496
#define MAP 497
#define MAPPING 498
#define MATCH 499
#define MATERIALIZED 500
#define MAXVALUE 501
#define METHOD 502
#define MICROSECOND_P 503
#define MICROSECONDS_P 504
#define MILLISECOND_P 505
#define MILLISECONDS_P 506
#define MINUTE_P 507
#define MINUTES_P 508
#define MINVALUE 509
#define MODE 510
#define MONTH_P 511
#define MONTHS_P 512
#define MOVE 513
#define NAME_P 514
#define NAMES 515
#define NATIONAL 516
#define NATURAL 517
#define NCHAR 518
#define NEW 519
#define NEXT 520
#define NO 521
#define NONE 522
#define NOT 523
#define NOTHING 524
#define NOTIFY 525
#define NOTNULL 526
#define NOWAIT 527
#define NULL_P 528
#define NULLIF 529
#define NULLS_P 530
#define NUMERIC 531
#define OBJECT_P 532
#define OF 533
#define OFF 534
#define OFFSET 535
#define OIDS 536
#define OLD 537
#define ON 538
#define ONLY 539
#define OPERATOR 540
#define OPTION 541
#define OPTIONS 542
#define OR 543
#define ORDER 544
#define ORDINALITY 545
#define OUT_P 546
#define OUTER_P 547
#define OVER 548
#define OVERLAPS 549
#define OVERLAY 550
#define OVERRIDING 551
#define OWNED 552
#define OWNER 553
#define PARALLEL 554
#define PARSER 555
#define PARTIAL 556
#define PARTITION 557
#define PASSING 558
#define PASSWORD 559
#define PERCENT 560
#define PIVOT 561
#define PIVOT_LONGER 562
#define PIVOT_WIDER 563
#define PKFK 564
#define PLACING 565
#define PLANS 566
#define POLICY 567
#define POSITION 568
#define POSITIONAL 569
#define PRAGMA_P 570
#define PRECEDING 571
#define PRECISION 572
#define PREPARE 573
#define PREPARED 574
#define PRESERVE 575
#define PRIMARY 576
#define PRIOR 577
#define PRIVILEGES 578
#define PROCEDURAL 579
#define PROCEDURE 580
#define PROGRAM 581
#define PUBLICATION 582
#define QUALIFY 583
#define QUOTE 584
#define RAI 585
#define RANGE 586
#define READ_P 587
#define REAL 588
#define REASSIGN 589
#define RECHECK 590
#define RECURSIVE 591
#define REF 592
#define REFERENCES 593
#define REFERENCING 594
#define REFRESH 595
#define REINDEX 596
#define RELATIVE_P 597
#define RELEASE 598
#define RENAME 599
#define REPEATABLE 600
#define REPLACE 601
#define REPLICA 602
#define RESET 603
#define RESPECT_P 604
#define RESTART 605
#define RESTRICT 606
#define RETURNING 607
#define RETURNS 608
#define REVOKE 609
#define RIGHT 610
#define ROLE 611
#define ROLLBACK 612
#define ROLLUP 613
#define ROW 614
#define ROWS 615
#define RULE 616
#define SAMPLE 617
#define SAVEPOINT 618
#define SCHEMA 619
#define SCHEMAS 620
#define SCROLL 621
#define SEARCH 622
#define SECOND_P 623
#define SECONDS_P 624
#define SECURITY 625
#define SELECT 626
#define SELF 627
#define SEMI 628
#define SEQUENCE 629
#define SEQUENCES 630
#define SERIALIZABLE 631
#define SERVER 632
#define SESSION 633
#define SET 634
#define SETOF 635
#define SETS 636
#define SHARE 637
#define SHOW 638
#define SIMILAR 639
#define SIMPLE 640
#define SKIP 641
#define SMALLINT 642
#define SNAPSHOT 643
#define SOME 644
#define SQL_P 645
#define STABLE 646
#define STANDALONE_P 647
#define START 648
#define STATEMENT 649
#define STATISTICS 650
#define STDIN 651
#define STDOUT 652
#define STORAGE 653
#define STORED 654
#define STRICT_P 655
#define STRIP_P 656
#define STRUCT 657
#define SUBSCRIPTION 658
#define SUBSTRING 659
#define SUMMARIZE 660
#define SYMMETRIC 661
#define SYSID 662
#define SYSTEM_P 663
#define TABLE 664
#define TABLES 665
#define TABLESAMPLE 666
#define TABLESPACE 667
#define TEMP 668
#define TEMPLATE 669
#define TEMPORARY 670
#define TEXT_P 671
#define THEN 672
#define TIME 673
#define TIMESTAMP 674
#define TO 675
#define TRAILING 676
#define TRANSACTION 677
#define TRANSFORM 678
#define TREAT 679
#define TRIGGER 680
#define TRIM 681
#define TRUE_P 682
#define TRUNCATE 683
#define TRUSTED 684
#define TRY_CAST 685
#define TYPE_P 686
#define TYPES_P 687
#define UNBOUNDED 688
#define UNCOMMITTED 689
#define UNDIRECTED 690
#define UNENCRYPTED 691
#define UNION 692
#define UNIQUE 693
#define UNKNOWN 694
#define UNLISTEN 695
#define UNLOGGED 696
#define UNPIVOT 697
#define UNTIL 698
#define UPDATE 699
#define USE_P 700
#define USER 701
#define USING 702
#define VACUUM 703
#define VALID 704
#define VALIDATE 705
#define VALIDATOR 706
#define VALUE_P 707
#define VALUES 708
#define VARCHAR 709
#define VARIADIC 710
#define VARYING 711
#define VERBOSE 712
#define VERSION_P 713
#define VIEW 714
#define VIEWS 715
#define VIRTUAL 716
#define VOLATILE 717
#define WHEN 718
#define WHERE 719
#define WHITESPACE_P 720
#define WINDOW 721
#define WITH 722
#define WITHIN 723
#define WITHOUT 724
#define WORK 725
#define WRAPPER 726
#define WRITE_P 727
#define XML_P 728
#define XMLATTRIBUTES 729
#define XMLCONCAT 730
#define XMLELEMENT 731
#define XMLEXISTS 732
#define XMLFOREST 733
#define XMLNAMESPACES 734
#define XMLPARSE 735
#define XMLPI 736
#define XMLROOT 737
#define XMLSERIALIZE 738
#define XMLTABLE 739
#define YEAR_P 740
#define YEARS_P 741
#define YES_P 742
#define ZONE 743
#define NOT_LA 744
#define NULLS_LA 745
#define WITH_LA 746
#define POSTFIXOP 747
#define UMINUS 748




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "third_party/libpg_query/grammar/grammar.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGCTEMaterialize			ctematerialize;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
}
/* Line 1529 of yacc.c.  */
#line 1083 "third_party/libpg_query/grammar/grammar_out.hpp"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


