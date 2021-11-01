#time "on"
#r "nuget: Akka.FSharp" 

open System
open System.Diagnostics
open Akka.FSharp
open Akka.Actor
open System.Collections.Generic

type MessageTypes =
    | StartGossip
    | Gossip
    | RetryGossip
    | Heartbeat
    | PushSum of Double * Double
    | StartPushSum
    | Stop
    | RetryPushSum

let mutable cubeRoot = 0
let pushSumDelta: double = Math.Pow(10.0, -10.0)

let stopWatch = Stopwatch()

let getNextPerfectCube(numNodes) = 
    let cbroot:int = int(Math.Ceiling(Math.Cbrt(float(numNodes)))**3.0)
    cbroot

let system = ActorSystem.Create("Actor-System")
let mutable TotalWorkers = 0

let neighbors = new Dictionary<string, string[]>()
let actors = new Dictionary<string, IActorRef>()
let actorTerminated = new Dictionary<string, Boolean>()
let mutable liveActors = new List<_>()

let randomNum (length) = Random().Next(0, length)

let randomCoordinate(model: string, len: int) =
    if model.Contains("3D") then
        let index = Random().Next(0, liveActors.Count)
        liveActors.[index]
    else
        string(randomNum(len))
     
let getSum(k: string) = 
    let z = k.Split(",")
    let x = [for i in z do int(i)]
    List.sum x


let Actor(neighbors: string[], key: string, listener: IActorRef)(mailbox: Actor<_>) =
    let mutable rumourHeardCount = 0
    let mutable sum = getSum(key) |> float
    let mutable weight = 1.0
    let mutable delta = 0.0
    let mutable ratio = sum/weight
    let mutable convergeCount = 0
    let mutable converged = false
    let rec loop () = 
        actor {
                let! msg = mailbox.Receive()
                match msg with
                | Gossip ->
                    if rumourHeardCount < 11 then
                        rumourHeardCount <- rumourHeardCount + 1
                        if rumourHeardCount = 10 then
                            actorTerminated.[key] <- true
                            listener <! Heartbeat
                        else
                            let index = randomNum(neighbors.Length)
                            let trialNode = neighbors.[index]
                            if not (actorTerminated.Item(trialNode)) then
                                actors.[trialNode] <! Gossip
                            else
                                mailbox.Self <! RetryGossip
                | RetryGossip -> 
                    let index = randomNum(neighbors.Length)
                    let trialNode = neighbors.[index]
                    if not (actorTerminated.Item(trialNode)) then
                        actors.[trialNode] <! Gossip
                    else
                        mailbox.Self <! RetryGossip
                | PushSum(s, w) ->
                    let newSum = sum + s
                    let newWeight = weight + w
                    let newRatio = newSum / newWeight

                    if not converged then
                        delta <- newRatio - ratio |> abs
                        sum <- newSum / 2.0
                        weight <- newWeight / 2.0
                        ratio <- newRatio
                        if delta > pushSumDelta then
                            convergeCount <- 0
                        else
                            convergeCount <- convergeCount + 1
                            if convergeCount = 3 then
                                converged <- true
                                actorTerminated.[key] <- true
                                listener <! Heartbeat
                            else
                                let index = randomNum(neighbors.Length)
                                let trialNode = neighbors.[index]
                                actors.[trialNode] <! PushSum(sum, weight)
                | RetryPushSum -> 
                    let index = randomNum(neighbors.Length)
                    let trialNode = neighbors.[index]
                    if not (actorTerminated.Item(trialNode)) then
                        actors.[trialNode] <! PushSum(sum, weight)
                    else
                        mailbox.Self <! RetryPushSum
                | _ -> printfn "Invalid response(Worker)"

                return! loop()
        }
    loop()

let Supervisor(model: string, len: int)(mailbox: Actor<_>) =
    let timer = new Timers.Timer(float 100)
    let rec loop () = 
        actor {
                let! msg = mailbox.Receive()
                match msg with
                | StartGossip ->
                    Console.WriteLine("Gossip started...")
                    stopWatch.Start()
                    let randomNode = randomCoordinate(model, len)
                    actors.[randomNode] <! Gossip
                    timer.AutoReset <- true
                    let eventHandler _ = mailbox.Self <!  RetryGossip
                    timer.Elapsed.Add eventHandler
                    timer.Start()
                | StartPushSum ->
                    Console.WriteLine("PushSum started...")
                    stopWatch.Start()
                    actors.Item(randomCoordinate(model, len)) <! PushSum(0.0,0.0)
                    timer.AutoReset <- true
                    let eventHandler _ = mailbox.Self <!  RetryPushSum
                    timer.Elapsed.Add eventHandler
                    timer.Start()
                | RetryGossip ->
                    if liveActors.Count > 0 then
                        let randomNode = randomCoordinate(model, len)
                        if not (actorTerminated.Item(randomNode)) then
                            actors.[randomNode] <! Gossip
                        else
                            liveActors.Remove randomNode |> ignore
                        mailbox.Self <! RetryGossip
                | Stop ->
                    timer.Stop()
                | RetryPushSum ->
                    if liveActors.Count > 0 then
                        let randomNode = randomCoordinate(model, len)
                        if not (actorTerminated.Item(randomNode)) then
                            actors.[randomNode] <! PushSum(0.0, 0.0)
                        else
                            liveActors.Remove randomNode |> ignore
                        mailbox.Self <! RetryPushSum
                | _ -> printfn "Invalid response(Supervisor)"
                return! loop()
            }
    loop()

let Listener(numNodes: int, supervisor: IActorRef)(mailbox: Actor<_>) = 
    let mutable listenCount = 0
    let rec loop () = 
        actor {
                let! msg = mailbox.Receive()
                match msg with
                | Heartbeat ->
                    listenCount <- listenCount + 1
                    if listenCount = numNodes then 
                        supervisor.Tell(Stop, mailbox.Self)
                        stopWatch.Stop()
                        printfn "%d Proper!!" listenCount
                        printfn "Time to complete for convergence: {%d} ms" stopWatch.ElapsedMilliseconds
                        mailbox.Context.System.Terminate() |> ignore
                | _ -> printfn "Invalid response(Supervisor)"
                return! loop()
            }
    loop()

let createAdjacencyListFor3DGrid(cubeRoot: int, neighbors: Dictionary<string, string[]>, improperFlag: Boolean) =
    for i in 0 .. cubeRoot-1 do
        for j in 0 .. cubeRoot-1 do
            for k in 0 .. cubeRoot-1 do
                let key = string(i) + "," + string(j) + "," + string(k)
                let mutable adjList : string list = []
                if i = 0 then adjList <-  string(i+1) + "," + string(j) + "," + string(k) :: adjList
                else if i = cubeRoot - 1 then adjList <-  string(i-1) + "," + string(j) + "," + string(k) :: adjList
                else
                    adjList <-  string(i-1) + "," + string(j) + "," + string(k) :: adjList
                    adjList <-  string(i+1) + "," + string(j) + "," + string(k) :: adjList
                if j = 0 then adjList <-  string(i) + "," + string(j+1) + "," + string(k) :: adjList
                else if j = cubeRoot - 1 then adjList <-  string(i) + "," + string(j-1) + "," + string(k) :: adjList
                else
                    adjList <-  string(i) + "," + string(j-1) + "," + string(k) :: adjList
                    adjList <-  string(i) + "," + string(j+1) + "," + string(k) :: adjList
                if k = 0 then adjList <-  string(i) + "," + string(j) + "," + string(k+1) :: adjList
                else if k = cubeRoot - 1 then adjList <-  string(i) + "," + string(j) + "," + string(k-1) :: adjList
                else
                    adjList <-  string(i) + "," + string(j) + "," + string(k-1) :: adjList
                    adjList <-  string(i) + "," + string(j) + "," + string(k+1) :: adjList
                if improperFlag then
                    adjList <- (string(randomNum(cubeRoot)) + "," + string(randomNum(cubeRoot)) + "," + string(randomNum(cubeRoot))) :: adjList
                let adjArray = adjList |> List.toArray
                neighbors.Add(key, adjArray)

let create3DGridActors(cubeRoot: int, neighbors: Dictionary<string, string[]>, actors: Dictionary<string, IActorRef>, listener: IActorRef) = 
    for i in 0 .. cubeRoot-1 do
        for j in 0 .. cubeRoot-1 do
            for k in 0 .. cubeRoot-1 do
                let key =  string(i) + "," + string(j) + "," + string(k)
                let workerId = "worker-" +  key
                actors.Add(key, spawn system workerId (Actor(neighbors.Item(key), key, listener)))
                actorTerminated.Add(key, false)
                liveActors.Add(key)

let createImperfect3DActorGrid(cubeRoot: int, neighbors: Dictionary<string, string[]>, actors: Dictionary<string, IActorRef>, listener: IActorRef) = 
    let x = create3DGridActors(cubeRoot, neighbors, actors, listener)
    x
   
let createAdjacencyListForLineNetwork(nodeCount: int) = 
    for i in 0 .. nodeCount-1 do
        let mutable adjList : string list = []
        if i = 0 then
            adjList <- string(i+1) :: adjList
        else if 0 < i  && i < (nodeCount - 1) then 
            adjList <- string(i-1) :: adjList
            adjList <- string(i+1) :: adjList
        else
            adjList <- string(i-1) :: adjList
        let x = adjList |> List.toArray
        neighbors.Add(string(i), x)

let createLineActors(numNodes:int, neighbors: Dictionary<string, string[]>, actors: Dictionary<string, IActorRef>, listener: IActorRef) = 
    for k in 0 .. numNodes-1 do
        let key =  string(k)
        let workerId = "worker-" +  key
        actors.Add(key, spawn system workerId (Actor(neighbors.Item(key), key, listener)))
        actorTerminated.Add(key, false)
        liveActors.Add(key);

let createAdjacencyListForFullNetwork(nodeCount: int) =
    for i in 0 .. nodeCount-1 do
        let mutable adjList : string list = []
        for j in 0 .. nodeCount-1 do
            if i <> j then 
                adjList <- string(j) :: adjList
        let x = adjList |> List.toArray
        neighbors.Add(string(i), x)

let createFullNetworkActors(numNodes:int, neighbors: Dictionary<string, string[]>, actors: Dictionary<string, IActorRef>, listener: IActorRef) = 
    createLineActors(numNodes, neighbors, actors, listener)

let main(numNodes, topology: string, algorithm) =
    if topology.Contains "3D" then
        let cube = getNextPerfectCube(numNodes)
        TotalWorkers <- cube
        cubeRoot <- int(Math.Cbrt(float(cube)))
    else
        TotalWorkers <- numNodes

    if topology = "Imp3D" && algorithm = "gossip" then
        createAdjacencyListFor3DGrid(cubeRoot, neighbors, true)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, cubeRoot))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        create3DGridActors(cubeRoot, neighbors, actors, listener)
        supervisor <! StartGossip
    else if topology = "3D" && algorithm = "gossip" then
        createAdjacencyListFor3DGrid(cubeRoot, neighbors, false)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, cubeRoot))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        create3DGridActors(cubeRoot, neighbors, actors, listener)
        supervisor <! StartGossip
    else if topology = "Line" && algorithm = "gossip" then
        createAdjacencyListForLineNetwork(numNodes)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, numNodes))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        createLineActors(TotalWorkers, neighbors, actors, listener)
        supervisor <! StartGossip
    else if topology = "Full" && algorithm = "gossip" then
        createAdjacencyListForFullNetwork(numNodes)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, numNodes))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        createFullNetworkActors(TotalWorkers, neighbors, actors, listener)
        supervisor <! StartGossip
    else if topology = "Imp3D" && algorithm = "pushsum" then
        createAdjacencyListFor3DGrid(cubeRoot, neighbors, true)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, cubeRoot))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        create3DGridActors(cubeRoot, neighbors, actors, listener)
        supervisor <! StartPushSum
    else if topology = "3D" && algorithm = "pushsum" then
        createAdjacencyListFor3DGrid(cubeRoot, neighbors, false)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, cubeRoot))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        create3DGridActors(cubeRoot, neighbors, actors, listener)
        supervisor <! StartPushSum
    else if topology = "Line" && algorithm = "pushsum" then
        createAdjacencyListForLineNetwork(numNodes)
        let supervisor = spawn system "Supervisor" (Supervisor(topology, numNodes))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        createLineActors(TotalWorkers, neighbors, actors, listener)
        supervisor <! StartPushSum
    else if topology = "Full" && algorithm = "pushsum" then
        createAdjacencyListForFullNetwork(numNodes)
        TotalWorkers <- numNodes
        let supervisor = spawn system "Supervisor" (Supervisor(topology, numNodes))
        let listener = spawn system "listener" (Listener(TotalWorkers, supervisor))
        createFullNetworkActors(TotalWorkers, neighbors, actors, listener)
        supervisor <! StartPushSum
    system.WhenTerminated.Wait()      

match fsi.CommandLineArgs with 
    | [|_; numNodes; topology; algorithm|] -> main(int(numNodes), topology, algorithm)
    | _ -> printfn "Error: Invalid Arguments."
            