- puffgres should not be generaintg a project, it should be something that you initiate within an existing project. 
for example, cd my-project, puffgres init (which just generates a directory called puffgres)
- puffgres cli should generate the .env file that is .gitignored
- it should ask me the name of the migration when it initializes puffgres
- in the command line, it should say: "Creating __puffgres_backfill, __puffgres_dlq" etc, all of the puffgres stuff. and should
confirm with a yes/no prompt 
- before you backfill or migrate, btw, you should check to see that our local migration directory is the same as the one in puffgres 
- when you do puffgres migrate, it should also give a CLI prompt to let you type in the migration title.  the transform should also be 
named based on whatever this is.
- puffgres should have a PUFFGRES_BASE_NAMESPACE env variable, i.e. PRODUCTION or DEVELOPMENT, so you can run prod and dev migrations
- there should be a new CLI command called puffgres reset, that pulls the migrations from the database down into toml
- we should enforce versioned transforms. maybe in init you should do this, but, basically:
transforms should be immutable. i think you should store the formatted typescript in a new table you init and manage called __puffgres_transforms. when you do puffgres reset, it should also pull those transforms down into the user directory (and delete the others). also, it should have a hash of them, so that if you try to migrate or backfill and do so in a way that violates immutability ot changes old files, it will throw an error, in red in the terminal, and say why. i.e. this should be red Error: Cannot proceed: applied migrations have been modified locally. Run `puffgres reset` to reset your config
- there should be something called `puffgres dangerously-delete-config`. it should "Are you sure you want to run this? Puffgres will remove all artifacts of itself, deleting internal tracking tables: {LIST OF THEM} in Postgres, and will remove its own configurations" basically that it will remove all of the artifacts of itself, deleting the stuff in postgres. it should maek you type "Yes, I want to dangerously deleting my puffgres config, in particular the configurations in Postgres, my transforms, and my migrations directory, and I know that it may be difficult to recover."
- there should be something called `puffgres dangerously-reset-turbopuffer` that should say "Are you sure you want to run this? Puffgres will delete all of the target namespaces you've referenced in migrations or configs." It should make you type out "Yes, I want to dangerously delete all of my referenced turbopuffer namespaces. I may need to recreate these, redo backfills, or lose data in the process, and it may be hard to recover."
