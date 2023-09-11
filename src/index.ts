async function run() {
  console.log("Hello World");
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
